use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::metadata::{
    SqlInfoData, SqlInfoDataBuilder, XdbcTypeInfo, XdbcTypeInfoData, XdbcTypeInfoDataBuilder,
};
use arrow_flight::sql::{
    server::FlightSqlService, ActionBeginSavepointRequest, ActionBeginSavepointResult,
    ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionCancelQueryRequest,
    ActionCancelQueryResult, ActionClosePreparedStatementRequest,
    ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult,
    ActionCreatePreparedSubstraitPlanRequest, ActionEndSavepointRequest,
    ActionEndTransactionRequest, Any, CommandGetCatalogs, CommandGetCrossReference,
    CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    CommandStatementSubstraitPlan, CommandStatementUpdate, Nullable, ProstMessageExt, Searchable,
    SqlInfo, TicketStatementQuery, XdbcDataType,
};
use arrow_flight::{
    flight_service_server::FlightService, Action, FlightData, FlightDescriptor, FlightEndpoint,
    FlightInfo, HandshakeRequest, HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket,
};
use async_trait::async_trait;
use columnq::arrow_schema::Schema;
use columnq::datafusion::arrow::ipc::writer::IpcWriteOptions;
use columnq::datafusion::arrow::record_batch::RecordBatch;
use columnq::datafusion::logical_expr::LogicalPlan;
use columnq::datafusion::prelude::{DataFrame, SessionContext};
use dashmap::DashMap;
use futures::{Stream, StreamExt, TryStreamExt};
use log::{debug, info};
use once_cell::sync::Lazy;
use prost::Message;
use snafu::prelude::*;
use snafu::{whatever, Whatever};
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use crate::config::Config;
use crate::context::RoapiContext;
use crate::server::RunnableServer;

// FIXME: uncomment for arrow 0.47 upgrade
// use arrow_flight::sql::server::PeekableFlightDataStream;
type PeekableFlightDataStream = Streaming<FlightData>;

macro_rules! internal_error {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

// TODO: retrive default catalog and schema from datafusion context
// see: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionState.html#method.resolve_table_references
const CATALOG_NAME: &str = "roapi";
const SCHEMA_NAME: &str = "public";
const FAKE_TOKEN: &str = "uuid_token";
const FAKE_UPDATE_RESULT: i64 = 1;

static INSTANCE_SQL_DATA: Lazy<SqlInfoData> = Lazy::new(|| {
    let mut builder = SqlInfoDataBuilder::new();
    // Server information
    builder.append(SqlInfo::FlightSqlServerName, "RoAPI Flight SQL Server");
    builder.append(SqlInfo::FlightSqlServerVersion, "1");
    // 1.3 comes from https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/Schema.fbs#L24
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.build().unwrap()
});

static INSTANCE_XBDC_DATA: Lazy<XdbcTypeInfoData> = Lazy::new(|| {
    let mut builder = XdbcTypeInfoDataBuilder::new();
    builder.append(XdbcTypeInfo {
        type_name: "INTEGER".into(),
        data_type: XdbcDataType::XdbcInteger,
        column_size: Some(32),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: false,
        searchable: Searchable::Full,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_increment: Some(false),
        local_type_name: Some("INTEGER".into()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcInteger,
        datetime_subcode: None,
        num_prec_radix: Some(2),
        interval_precision: None,
    });
    builder.build().unwrap()
});

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read TLS file: {source}"))]
    ReadTlsFile { source: std::io::Error },
}

pub struct RoapiFlightSqlService<H: RoapiContext> {
    ctx: Arc<H>,
    statements: Arc<DashMap<String, LogicalPlan>>,
    results: Arc<DashMap<String, Vec<RecordBatch>>>,
}

impl<H: RoapiContext> RoapiFlightSqlService<H> {
    fn new(ctx: Arc<H>) -> Self {
        Self {
            ctx,
            statements: Arc::new(DashMap::new()),
            results: Arc::new(DashMap::new()),
        }
    }

    async fn get_ctx<T>(&self, req: &Request<T>) -> Result<SessionContext, Status> {
        self.check_token(req)?;

        Ok(self.ctx.get_dfctx().await)
    }

    fn get_result(&self, handle: &str) -> Result<Vec<RecordBatch>, Status> {
        if let Some(result) = self.results.get(handle) {
            Ok(result.clone())
        } else {
            Err(Status::internal(format!(
                "Request handle not found: {handle}"
            )))?
        }
    }

    fn get_plan(&self, handle: &str) -> Result<LogicalPlan, Status> {
        if let Some(plan) = self.statements.get(handle) {
            Ok(plan.clone())
        } else {
            Err(Status::internal(format!("Plan handle not found: {handle}")))?
        }
    }

    fn remove_plan(&self, handle: &str) -> Result<(), Status> {
        self.statements.remove(&handle.to_string());
        Ok(())
    }

    fn remove_result(&self, handle: &str) -> Result<(), Status> {
        self.results.remove(handle);
        Ok(())
    }

    fn check_token<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let metadata = req.metadata();
        if let Some(auth) = metadata.get("authorization") {
            let s = auth
                .to_str()
                .map_err(|e| Status::internal(format!("Error parsing header: {e}")))?;

            let authorization = s.to_string();
            let bearer = "Bearer ";
            if !authorization.starts_with(bearer) {
                Err(Status::internal("Invalid auth header!"))?;
            }
            let token = authorization[bearer.len()..].to_string();
            if token == FAKE_TOKEN {
                return Ok(());
            } else {
                return Err(Status::unauthenticated("invalid token "));
            }
        }

        Ok(())
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResults {
    #[prost(string, tag = "1")]
    pub handle: ::prost::alloc::string::String,
}

impl ProstMessageExt for FetchResults {
    fn type_url() -> &'static str {
        "type.googleapis.com/roapi.example.com.sql.FetchResults"
    }

    fn as_any(&self) -> Any {
        Any {
            type_url: FetchResults::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}

#[tonic::async_trait]
impl<H: RoapiContext> FlightSqlService for RoapiFlightSqlService<H> {
    type FlightService = RoapiFlightSqlService<H>;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let result = HandshakeResponse {
            protocol_version: 0,
            payload: FAKE_TOKEN.into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        return Ok(Response::new(Box::pin(output)));
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.check_token(&request)?;

        if !message.is::<FetchResults>() {
            Err(Status::unimplemented(format!(
                "do_get_fallback: The defined request is invalid: {:?}",
                message
            )))?
        }

        let fr: FetchResults = message
            .unpack()
            .map_err(|e| Status::internal(format!("{e:?}")))?
            .ok_or_else(|| Status::internal("Expected FetchResults but got None!"))?;

        let handle = fr.handle;

        info!("getting results for {handle}");
        let result = self.get_result(&handle)?;
        // if we get an empty result, create an empty schema
        let (schema, batches) = match result.get(0) {
            None => (Arc::new(Schema::empty()), vec![]),
            Some(batch) => (batch.schema(), result.clone()),
        };

        let batch_stream = futures::stream::iter(batches).map(Ok);

        // TODO: look into use arrow_flight::utils::batches_to_flight_data;
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(Status::from);

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check_token(&request)?;

        debug!("got flight_info_statement user query: {:#?}", &query);
        let user_query = query.query.as_str();
        info!(
            "get_flight_info_statement, executing query: `{}`",
            &user_query
        );

        let ctx = self.get_ctx(&request).await?;

        let plan = ctx
            .sql(user_query)
            .await
            .and_then(|df| df.into_optimized_plan())
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?;

        let state = ctx.state();
        let df = DataFrame::new(state, plan);
        let result = df
            .collect()
            .await
            .map_err(|e| internal_error!("Error executing query", e))?;

        // if we get an empty result, create an empty schema
        let schema = match result.get(0) {
            None => Schema::empty(),
            Some(batch) => (*batch.schema()).clone(),
        };

        let handle = Uuid::new_v4().hyphenated().to_string();
        self.results.insert(handle.clone(), result);

        let fetch = FetchResults { handle };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            // if we had multiple endpoints to connect to, we could use this Location
            // but in the case of standalone DataFusion, we don't
            // let loc = arrow_flight::Location {
            //     uri: "grpc+tcp://127.0.0.1:50051".to_string(),
            // };
            location: vec![],
        };
        let endpoints = vec![endpoint];

        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| internal_error!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Default::default(),
            path: vec![],
        };
        // send -1 for total_records and total_bytes instead of iterating over all the
        // batches to get num_rows() and total byte size.
        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(flight_desc),
            endpoint: endpoints,
            total_records: -1_i64,
            total_bytes: -1_i64,
            ordered: false,
        };
        let resp = Response::new(info);
        Ok(resp)
    }

    async fn get_flight_info_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_substrait_plan not implemented",
        ))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check_token(&request)?;

        info!("get_flight_info_prepared_statement");
        let handle = std::str::from_utf8(&cmd.prepared_statement_handle)
            .map_err(|e| internal_error!("Unable to parse uuid", e))?;

        let ctx = self.get_ctx(&request).await?;
        let plan = self.get_plan(handle)?;

        let state = ctx.state();
        let df = DataFrame::new(state, plan);
        let result = df
            .collect()
            .await
            .map_err(|e| internal_error!("Error executing query", e))?;

        // if we get an empty result, create an empty schema
        let schema = match result.get(0) {
            None => Schema::empty(),
            Some(batch) => (*batch.schema()).clone(),
        };

        self.results.insert(handle.to_string(), result);

        let fetch = FetchResults {
            handle: handle.to_string(),
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            // if we had multiple endpoints to connect to, we could use this Location
            // but in the case of standalone DataFusion, we don't
            // let loc = arrow_flight::Location {
            //     uri: "grpc+tcp://127.0.0.1:50051".to_string(),
            // };
            location: vec![],
        };
        let endpoints = vec![endpoint];

        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| internal_error!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Default::default(),
            path: vec![],
        };
        // send -1 for total_records and total_bytes instead of iterating over all the
        // batches to get num_rows() and total byte size.
        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(flight_desc),
            endpoint: endpoints,
            total_records: -1_i64,
            total_bytes: -1_i64,
            ordered: false,
        };
        let resp = Response::new(info);
        Ok(resp)
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: Any::pack(&query)
                .map_err(|e| internal_error!("unable to serialize get catalogs command", e))?
                .encode_to_vec()
                .into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| internal_error!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: Any::pack(&query)
                .map_err(|e| internal_error!("unable to serialize get schemas command", e))?
                .encode_to_vec()
                .into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| internal_error!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: Any::pack(&query)
                .map_err(|e| internal_error!("unable to serialize get tables command", e))?
                .encode_to_vec()
                .into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| internal_error!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
    }

    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_table_types not implemented",
        ))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: Any::pack(&query)
                .map_err(|e| internal_error!("unable to serialize get sql_info command", e))?
                .encode_to_vec()
                .into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&INSTANCE_SQL_DATA).schema().as_ref())
            .map_err(|e| internal_error!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
    }

    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_primary_keys not implemented",
        ))
    }

    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_exported_keys not implemented",
        ))
    }

    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&INSTANCE_XBDC_DATA).schema().as_ref())
            .map_err(|e| internal_error!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
    }

    // do_get
    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_statement not implemented"))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_prepared_statement not implemented",
        ))
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = query.into_builder();
        builder.append(CATALOG_NAME);
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = query.into_builder();
        builder.append(CATALOG_NAME, SCHEMA_NAME);

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // query.table_types can be one of TABLE, VIEW, and SYSTEM TABLE
        // TODO: ignore SYSTEM TABLE and VIEW
        let table_type = query
            .table_types
            .get(0)
            .map(|s| s.as_str())
            .unwrap_or_else(|| "table")
            .to_string();
        let include_schema = query.include_schema;

        let mut builder = query.into_builder();
        if include_schema {
            let schemas = self
                .ctx
                .schemas()
                .await
                .map_err(|e| internal_error!("Failed to get table schemas", e))?;
            for (table_name, table_schema) in schemas.into_iter() {
                builder
                    .append(
                        CATALOG_NAME,
                        SCHEMA_NAME,
                        table_name,
                        &table_type,
                        table_schema.as_ref(),
                    )
                    .map_err(Status::from)?;
            }
        } else {
            let table_schema = Schema::empty();
            for table_name in self.ctx.table_names().await.into_iter() {
                builder
                    .append(
                        CATALOG_NAME,
                        SCHEMA_NAME,
                        table_name,
                        &table_type,
                        &table_schema,
                    )
                    .map_err(Status::from)?;
            }
        };

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_table_types not implemented"))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let builder = query.into_builder(&INSTANCE_SQL_DATA);
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_primary_keys not implemented"))
    }

    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_exported_keys not implemented",
        ))
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_imported_keys not implemented",
        ))
    }

    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_cross_reference not implemented",
        ))
    }

    async fn do_get_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // create a builder with pre-defined Xdbc data:
        let builder = query.into_builder(&INSTANCE_XBDC_DATA);
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    // do_put
    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Ok(FAKE_UPDATE_RESULT)
    }

    async fn do_put_substrait_plan(
        &self,
        _ticket: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_substrait_plan not implemented",
        ))
    }

    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "do_put_prepared_statement_query not implemented",
        ))
    }

    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_prepared_statement_update not implemented",
        ))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        self.check_token(&request)?;

        let user_query = query.query.as_str();
        info!("do_action_create_prepared_statement: {user_query}");

        let ctx = self.get_ctx(&request).await?;

        let plan = ctx
            .sql(user_query)
            .await
            .and_then(|df| df.into_optimized_plan())
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?;

        // store a copy of the plan,  it will be used for execution
        let plan_uuid = Uuid::new_v4().hyphenated().to_string();
        self.statements.insert(plan_uuid.clone(), plan.clone());

        let plan_schema = plan.schema();

        let arrow_schema = (&**plan_schema).into();
        let message = SchemaAsIpc::new(&arrow_schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| internal_error!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: plan_uuid.into(),
            dataset_schema: schema_bytes,
            parameter_schema: Default::default(),
        };

        Ok(res)
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        let handle = std::str::from_utf8(&query.prepared_statement_handle);
        if let Ok(handle) = handle {
            info!("do_action_close_prepared_statement: removing plan and results for {handle}");
            let _ = self.remove_plan(handle);
            let _ = self.remove_result(handle);
        }
        Ok(())
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "Implement do_action_create_prepared_substrait_plan",
        ))
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        Err(Status::unimplemented(
            "Implement do_action_begin_transaction",
        ))
    }

    async fn do_action_end_transaction(
        &self,
        _query: ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("Implement do_action_end_transaction"))
    }

    async fn do_action_begin_savepoint(
        &self,
        _query: ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        Err(Status::unimplemented("Implement do_action_begin_savepoint"))
    }

    async fn do_action_end_savepoint(
        &self,
        _query: ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("Implement do_action_end_savepoint"))
    }

    async fn do_action_cancel_query(
        &self,
        _query: ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        Err(Status::unimplemented("Implement do_action_cancel_query"))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

pub struct RoapiFlightSqlServer<H: RoapiContext> {
    pub ctx: Arc<H>,
    pub addr: std::net::SocketAddr,
    pub tls_config: Option<ServerTlsConfig>,
}

fn tonic_server_builder<H: RoapiContext>(
    ctx: Arc<H>,
    tls_config: &Option<ServerTlsConfig>,
) -> Result<tonic::transport::server::Router<tower_layer::Identity>, Whatever> {
    let svc = FlightServiceServer::with_interceptor(
        RoapiFlightSqlService::new(ctx),
        |req: Request<()>| -> Result<Request<()>, Status> {
            info!("FlightSQL req: {:?}", &req);
            Ok(req)
        },
    );
    let mut builder = tonic::transport::Server::builder();
    if let Some(cfg) = tls_config {
        builder = whatever!(
            builder.tls_config(cfg.clone()),
            "Failed to build TLS config"
        );
    }

    Ok(builder.add_service(svc))
}

impl<H: RoapiContext> RoapiFlightSqlServer<H> {
    pub async fn new(ctx: Arc<H>, config: &Config, default_host: String) -> Result<Self, Error> {
        let default_addr = format!("{default_host}:32010");
        let addr = config
            .addr
            .flight_sql
            .clone()
            .unwrap_or_else(|| default_addr.to_string());

        let listener = TcpListener::bind(addr)
            .await
            .expect("Failed to bind address for FlightSQL server");

        let tls_config = config
            .flight_sql_config
            .as_ref()
            .and_then(|cfg| {
                cfg.tls.as_ref().map(|tls_cfg| {
                    let cert = std::fs::read_to_string(&tls_cfg.cert).context(ReadTlsFileSnafu)?;
                    let key = std::fs::read_to_string(&tls_cfg.key).context(ReadTlsFileSnafu)?;
                    let client_ca =
                        std::fs::read_to_string(&tls_cfg.client_ca).context(ReadTlsFileSnafu)?;

                    Ok(ServerTlsConfig::new()
                        .identity(Identity::from_pem(cert, key))
                        .client_ca_root(Certificate::from_pem(client_ca)))
                })
            })
            .transpose()?;

        Ok(Self {
            ctx,
            addr: listener
                .local_addr()
                .expect("Failed to get address from listener"),
            tls_config,
        })
    }
}

#[async_trait]
impl<H: RoapiContext> RunnableServer for RoapiFlightSqlServer<H> {
    fn addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    async fn run(&self) -> Result<(), Whatever> {
        let router = tonic_server_builder(self.ctx.clone(), &self.tls_config)?;
        let listener = whatever!(
            TcpListener::bind(self.addr).await,
            "Failed to bind address for FlightSQL server",
        );

        let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
            .expect("Failed to create incoming TCP Router");

        whatever!(
            router.serve_with_incoming(incoming).await,
            "Failed to start FlightSQL server on."
        );
        Ok(())
    }
}
