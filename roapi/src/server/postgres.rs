// Wire protocol reference:
// https://www.postgresql.org/docs/current/protocol-message-formats.html
// https://beta.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf

use async_trait::async_trait;
use std::fmt::Debug;
use std::sync::Arc;

use columnq::datafusion::arrow::array::{
    BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, DurationMicrosecondArray,
    DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, Float16Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, StringArray, Time32MillisecondArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use columnq::datafusion::arrow::datatypes::{DataType, Schema};
use columnq::datafusion::error::DataFusionError;
use futures::stream;
use futures::Sink;
use log::info;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, ClientPortalStore, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;
use snafu::{whatever, Whatever};
use tokio::net::TcpListener;

use crate::config::Config;
use crate::context::RoapiContext;
use crate::server::RunnableServer;

// Convert DataFusion error to PgWire error
fn df_err_to_pg(err: DataFusionError) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        err.to_string(),
    )))
}

// Convert Arrow DataType to PostgreSQL Type
fn arrow_type_to_pg_type(data_type: &DataType) -> Type {
    match data_type {
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::CHAR,
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::UInt8 => Type::INT2,
        DataType::UInt16 => Type::INT4,
        DataType::UInt32 => Type::INT8,
        DataType::UInt64 => Type::INT8,
        DataType::Float16 => Type::FLOAT4,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 => Type::VARCHAR,
        DataType::LargeUtf8 => Type::VARCHAR,
        DataType::Binary => Type::BYTEA,
        DataType::LargeBinary => Type::BYTEA,
        DataType::Date32 => Type::DATE,
        DataType::Date64 => Type::DATE,
        DataType::Time32(_) => Type::TIME,
        DataType::Time64(_) => Type::TIME,
        DataType::Timestamp(_, _) => Type::TIMESTAMP,
        DataType::Duration(_) => Type::INTERVAL,
        DataType::Decimal128(_, _) => Type::NUMERIC,
        DataType::Decimal256(_, _) => Type::NUMERIC,
        DataType::Null => Type::UNKNOWN,
        DataType::FixedSizeBinary(_) => Type::BYTEA,
        DataType::List(_) => Type::TEXT, // JSON representation
        DataType::FixedSizeList(_, _) => Type::TEXT, // JSON representation
        DataType::LargeList(_) => Type::TEXT, // JSON representation
        DataType::Struct(_) => Type::TEXT, // JSON representation
        DataType::Map(_, _) => Type::TEXT, // JSON representation
        _ => Type::VARCHAR,              // Default to VARCHAR
    }
}

// Convert Arrow schema to PostgreSQL field info
fn schema_to_field_info(schema: &Schema) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .map(|field| {
            FieldInfo::new(
                field.name().clone(),
                None,
                None,
                arrow_type_to_pg_type(field.data_type()),
                FieldFormat::Text,
            )
        })
        .collect()
}

pub struct RoapiQueryHandler<H: RoapiContext> {
    ctx: Arc<H>,
}

impl<H: RoapiContext> RoapiQueryHandler<H> {
    fn new(ctx: Arc<H>) -> Self {
        Self { ctx }
    }

    async fn execute_query(&self, query: &str) -> PgWireResult<QueryResponse<'static>> {
        info!("executing query: {query}");

        // Handle some special PostgreSQL queries
        if query.trim().to_lowercase().starts_with("show") {
            let empty_schema = Arc::new(vec![]);
            let empty_stream = stream::iter(vec![]);
            return Ok(QueryResponse::new(empty_schema, empty_stream));
        }

        let df = self.ctx.sql_to_df(query).await.map_err(df_err_to_pg)?;
        let schema = df.schema().as_arrow().clone();
        let field_info = schema_to_field_info(&schema);

        let batches = df.collect().await.map_err(df_err_to_pg)?;
        let field_info_arc = Arc::new(field_info);

        let mut rows = Vec::new();
        for batch in &batches {
            for row_idx in 0..batch.num_rows() {
                let mut encoder = DataRowEncoder::new(field_info_arc.clone());
                for col_idx in 0..batch.num_columns() {
                    let column = batch.column(col_idx);
                    if column.is_null(row_idx) {
                        encoder.encode_field(&None::<&str>)?;
                        continue;
                    }
                    // Simple string conversion for all types
                    let value_str = match column.data_type() {
                        DataType::Utf8 => {
                            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::LargeUtf8 => {
                            let array = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Int8 => {
                            let array = column.as_any().downcast_ref::<Int8Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Int16 => {
                            let array = column.as_any().downcast_ref::<Int16Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Int32 => {
                            let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Int64 => {
                            let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::UInt8 => {
                            let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::UInt16 => {
                            let array = column.as_any().downcast_ref::<UInt16Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::UInt32 => {
                            let array = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::UInt64 => {
                            let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Float32 => {
                            let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Float64 => {
                            let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Boolean => {
                            let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Date32 => {
                            let array = column.as_any().downcast_ref::<Date32Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Date64 => {
                            let array = column.as_any().downcast_ref::<Date64Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Time32(_) => {
                            let array = column
                                .as_any()
                                .downcast_ref::<Time32MillisecondArray>()
                                .unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Time64(_) => {
                            let array = column
                                .as_any()
                                .downcast_ref::<Time64MicrosecondArray>()
                                .unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Timestamp(unit, _) => match unit {
                            columnq::datafusion::arrow::datatypes::TimeUnit::Second => {
                                let array = column
                                    .as_any()
                                    .downcast_ref::<TimestampSecondArray>()
                                    .unwrap();
                                array.value(row_idx).to_string()
                            }
                            columnq::datafusion::arrow::datatypes::TimeUnit::Millisecond => {
                                let array = column
                                    .as_any()
                                    .downcast_ref::<TimestampMillisecondArray>()
                                    .unwrap();
                                array.value(row_idx).to_string()
                            }
                            columnq::datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                                let array = column
                                    .as_any()
                                    .downcast_ref::<TimestampMicrosecondArray>()
                                    .unwrap();
                                array.value(row_idx).to_string()
                            }
                            columnq::datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                                let array = column
                                    .as_any()
                                    .downcast_ref::<TimestampNanosecondArray>()
                                    .unwrap();
                                array.value(row_idx).to_string()
                            }
                        },
                        DataType::Decimal128(_, _) => {
                            let array = column.as_any().downcast_ref::<Decimal128Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Binary => {
                            let array = column.as_any().downcast_ref::<BinaryArray>().unwrap();
                            let bytes = array.value(row_idx);
                            format!("\\x{}", hex::encode(bytes))
                        }
                        DataType::LargeBinary => {
                            let array = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                            let bytes = array.value(row_idx);
                            format!("\\x{}", hex::encode(bytes))
                        }
                        DataType::Duration(unit) => match unit {
                            columnq::datafusion::arrow::datatypes::TimeUnit::Second => {
                                let array = column
                                    .as_any()
                                    .downcast_ref::<DurationSecondArray>()
                                    .unwrap();
                                array.value(row_idx).to_string()
                            }
                            columnq::datafusion::arrow::datatypes::TimeUnit::Millisecond => {
                                let array = column
                                    .as_any()
                                    .downcast_ref::<DurationMillisecondArray>()
                                    .unwrap();
                                array.value(row_idx).to_string()
                            }
                            columnq::datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                                let array = column
                                    .as_any()
                                    .downcast_ref::<DurationMicrosecondArray>()
                                    .unwrap();
                                array.value(row_idx).to_string()
                            }
                            columnq::datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                                let array = column
                                    .as_any()
                                    .downcast_ref::<DurationNanosecondArray>()
                                    .unwrap();
                                array.value(row_idx).to_string()
                            }
                        },
                        DataType::Float16 => {
                            let array = column.as_any().downcast_ref::<Float16Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        DataType::Null => {
                            // Null arrays always return NULL, so this should never be reached for non-null values
                            // But if somehow we get here, return "NULL"
                            "NULL".to_string()
                        }
                        DataType::FixedSizeBinary(_) => {
                            // For FixedSizeBinary, we'll use the generic approach
                            format!("{:?}", column.slice(row_idx, 1))
                                .trim_start_matches('[')
                                .trim_end_matches(']')
                                .to_string()
                        }
                        DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::FixedSizeList(_, _) => {
                            // Convert complex arrays to JSON-like string representation
                            format!("{:?}", column.slice(row_idx, 1))
                                .trim_start_matches('[')
                                .trim_end_matches(']')
                                .to_string()
                        }
                        DataType::Struct(_) | DataType::Map(_, _) => {
                            // Convert complex structures to JSON-like string representation
                            format!("{:?}", column.slice(row_idx, 1))
                                .trim_start_matches('[')
                                .trim_end_matches(']')
                                .to_string()
                        }
                        _ => {
                            // Fallback: use the columnar format method
                            format!("{:?}", column.slice(row_idx, 1))
                                .trim_start_matches('[')
                                .trim_end_matches(']')
                                .to_string()
                        }
                    };
                    encoder.encode_field(&Some(value_str.as_str()))?;
                }
                rows.push(encoder.finish()?);
            }
        }

        let data_stream = stream::iter(rows.into_iter().map(Ok));
        Ok(QueryResponse::new(field_info_arc, data_stream))
    }
}

#[async_trait]
impl<H: RoapiContext> NoopStartupHandler for RoapiQueryHandler<H> {
    async fn post_startup<C>(
        &self,
        _client: &mut C,
        _message: pgwire::messages::PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        Ok(())
    }
}

#[async_trait]
impl<H: RoapiContext> SimpleQueryHandler for RoapiQueryHandler<H> {
    async fn do_query<'a, C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        // Split multiple statements
        let statements: Vec<&str> = query
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        let mut responses = Vec::new();

        for statement in statements {
            if statement.trim().is_empty() {
                continue;
            }

            match self.execute_query(statement).await {
                Ok(query_response) => {
                    responses.push(Response::Query(query_response));
                }
                Err(e) => {
                    responses.push(Response::Error(Box::new(e.into())));
                    break;
                }
            }
        }

        if responses.is_empty() {
            let empty_schema = Arc::new(vec![]);
            let empty_stream = stream::iter(vec![]);
            responses.push(Response::Query(QueryResponse::new(
                empty_schema,
                empty_stream,
            )));
        }

        Ok(responses)
    }
}

#[async_trait]
impl<H: RoapiContext> ExtendedQueryHandler for RoapiQueryHandler<H> {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser)
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: Send + Sync,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        let query = &portal.statement.statement;
        match self.execute_query(query).await {
            Ok(query_response) => Ok(Response::Query(query_response)),
            Err(e) => Ok(Response::Error(Box::new(e.into()))),
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: Send + Sync,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        let query = &stmt.statement;
        let df = self.ctx.sql_to_df(query).await.map_err(df_err_to_pg)?;
        let schema = df.schema().as_arrow().clone();
        let field_info = schema_to_field_info(&schema);

        Ok(DescribeStatementResponse::new(vec![], field_info))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: Send + Sync,
        C::Error: Debug,
        PgWireError: From<C::Error>,
    {
        let query = &portal.statement.statement;
        let df = self.ctx.sql_to_df(query).await.map_err(df_err_to_pg)?;
        let schema = df.schema().as_arrow().clone();
        let field_info = schema_to_field_info(&schema);

        Ok(DescribePortalResponse::new(field_info))
    }
}

struct RoapiHandlerFactory<H: RoapiContext> {
    handler: Arc<RoapiQueryHandler<H>>,
}

impl<H: RoapiContext> PgWireServerHandlers for RoapiHandlerFactory<H> {
    type StartupHandler = RoapiQueryHandler<H>;
    type SimpleQueryHandler = RoapiQueryHandler<H>;
    type ExtendedQueryHandler = RoapiQueryHandler<H>;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

pub struct PostgresServer<H: RoapiContext> {
    pub ctx: Arc<H>,
    pub addr: std::net::SocketAddr,
    pub listener: TcpListener,
}

impl<H: RoapiContext> PostgresServer<H> {
    pub async fn new(ctx: Arc<H>, config: &Config, default_host: String) -> Self {
        let default_addr = format!("{default_host}:5432");

        let addr = config
            .addr
            .postgres
            .clone()
            .unwrap_or_else(|| default_addr.to_string());

        let listener = TcpListener::bind(addr)
            .await
            .expect("Failed to bind address for Postgres server");
        Self {
            ctx,
            addr: listener
                .local_addr()
                .expect("Failed to get address from listener"),
            listener,
        }
    }
}

#[async_trait]
impl<H: RoapiContext> RunnableServer for PostgresServer<H> {
    fn addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    async fn run(&self) -> Result<(), Whatever> {
        loop {
            let (socket, _) = whatever!(
                self.listener.accept().await,
                "Failed to accept postgres connection"
            );

            let ctx = self.ctx.clone();
            tokio::spawn(async move {
                let handler = Arc::new(RoapiQueryHandler::new(ctx));
                let factory = Arc::new(RoapiHandlerFactory { handler });

                if let Err(e) = process_socket(socket, None, factory).await {
                    log::error!("Error processing postgres connection: {e}");
                }
            });
        }
    }
}
