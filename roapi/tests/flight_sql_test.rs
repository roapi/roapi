mod helpers;

use arrow_cast::pretty::pretty_format_batches;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::utils::flight_data_to_batches;
use arrow_flight::FlightData;
use arrow_flight::FlightInfo;
use arrow_ipc::convert::try_schema_from_ipc_buffer;
use columnq::arrow::datatypes::{DataType, Field};
use columnq::arrow_schema::ArrowError;
use columnq::datafusion::arrow;
use columnq::datafusion::arrow::record_batch::RecordBatch;
use columnq::table::TableSource;
use futures::TryStreamExt;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

fn endpoint(addr: std::net::SocketAddr) -> Result<Endpoint, ArrowError> {
    let uri = match addr {
        std::net::SocketAddr::V4(v) => {
            format!("http://{}:{}", v.ip(), v.port())
        }
        std::net::SocketAddr::V6(v) => {
            format!("http://[{}]:{}", v.ip(), v.port())
        }
    };
    let endpoint = Endpoint::new(uri)
        .expect("Cannot create endpoint")
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(20))
        .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
        .http2_keep_alive_interval(Duration::from_secs(300))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);

    Ok(endpoint)
}

async fn flight_info_to_batches(
    client: &mut FlightSqlServiceClient<Channel>,
    flight_info: FlightInfo,
) -> Vec<RecordBatch> {
    let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap().clone();
    let flight_data = client.do_get(ticket).await.unwrap();
    let flight_data: Vec<FlightData> = flight_data.try_collect().await.unwrap();

    flight_data_to_batches(&flight_data).unwrap()
}

async fn spawn_server_for_table(tables: Vec<TableSource>) -> std::net::SocketAddr {
    let (app, _) = helpers::test_api_app_with_tables(tables).await;
    let addr = app.flight_sql_addr();
    tokio::spawn(app.run_until_stopped());

    // give time for the server to start
    // TODO: remove magic sleep with proper signals
    tokio::time::sleep(Duration::from_millis(500)).await;

    addr
}

async fn get_flight_client(addr: std::net::SocketAddr) -> FlightSqlServiceClient<Channel> {
    let endpoint = endpoint(addr).unwrap();
    let channel = endpoint.connect().await.unwrap();
    FlightSqlServiceClient::new(channel)
}

#[tokio::test]
async fn test_flight_sql_spacex_aggregate() {
    let json_table = helpers::get_spacex_table();
    let addr = spawn_server_for_table(vec![json_table]).await;

    let mut client = get_flight_client(addr).await;
    let mut stmt = client
        .prepare(
            r#"SELECT COUNT(*), rocket as launch_cnt
               FROM spacex_launches
               GROUP BY rocket
               ORDER BY rocket"#
                .to_string(),
            None,
        )
        .await
        .unwrap();

    let flight_info = stmt.execute().await.unwrap();

    let batches = flight_info_to_batches(&mut client, flight_info).await;
    let res = pretty_format_batches(batches.as_slice()).unwrap();
    let expected = r#"
+----------+--------------------------+
| COUNT(*) | launch_cnt               |
+----------+--------------------------+
| 5        | 5e9d0d95eda69955f709d1eb |
| 122      | 5e9d0d95eda69973a809d1ec |
| 5        | 5e9d0d95eda69974db09d1ed |
+----------+--------------------------+"#
        .trim()
        .to_string();
    assert_eq!(res.to_string(), expected);
}

#[tokio::test]
async fn test_flight_sql_uk_cities_schema() {
    let csv_table = helpers::get_uk_cities_table();
    let addr = spawn_server_for_table(vec![csv_table]).await;

    let mut client = get_flight_client(addr).await;
    let flight_info = client.get_catalogs().await.unwrap();
    println!(">>>>>> {:#?}", &flight_info);

    let batches = flight_info_to_batches(&mut client, flight_info).await;
    let res = pretty_format_batches(batches.as_slice()).unwrap();
    let expected = r#"
+--------------+
| catalog_name |
+--------------+
| roapi        |
+--------------+"#
        .trim()
        .to_string();
    assert_eq!(res.to_string(), expected);
}

#[tokio::test]
async fn test_flight_sql_get_tables() {
    let json_table = helpers::get_spacex_table();
    let csv_table = helpers::get_uk_cities_table();
    let addr = spawn_server_for_table(vec![csv_table, json_table]).await;

    // Get table without schemas
    let mut client = get_flight_client(addr).await;
    let flight_info = client
        .get_tables(arrow_flight::sql::CommandGetTables {
            catalog: Some("roapi".to_string()),
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec!["BASE TABLE".to_string(), "TABLE".to_string()],
            include_schema: false,
        })
        .await
        .unwrap();

    let batches = flight_info_to_batches(&mut client, flight_info).await;
    let schema_column = batches[0].column_by_name("table_schema");
    assert!(schema_column.is_none());
    let res = pretty_format_batches(batches.as_slice()).unwrap();
    let expected = r#"
+--------------+----------------+-----------------+------------+
| catalog_name | db_schema_name | table_name      | table_type |
+--------------+----------------+-----------------+------------+
| roapi        | public         | spacex_launches | BASE TABLE |
| roapi        | public         | uk_cities       | BASE TABLE |
+--------------+----------------+-----------------+------------+"#
        .trim()
        .to_string();
    assert_eq!(res.to_string(), expected);

    // Get tables with schemas
    let flight_info = client
        .get_tables(arrow_flight::sql::CommandGetTables {
            catalog: Some("roapi".to_string()),
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec!["table".to_string()],
            include_schema: true,
        })
        .await
        .unwrap();

    let batches = flight_info_to_batches(&mut client, flight_info).await;
    let schema_column = batches[0].column_by_name("table_schema");
    assert!(schema_column.is_some());
    assert_eq!(batches[0].num_rows(), 2);

    // Get specific table with schemas
    let flight_info = client
        .get_tables(arrow_flight::sql::CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: Some("uk_cities".to_string()),
            table_types: vec![],
            include_schema: true,
        })
        .await
        .unwrap();

    let batches = flight_info_to_batches(&mut client, flight_info).await;
    let batch = &batches[0];
    let schema_arr = batch
        .column_by_name("table_schema")
        .expect("schema not found in response");
    assert_eq!(batch.num_rows(), 1);
    let schema_arr = schema_arr
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .expect("Schema column should be a binary array");
    let schema_bytes = schema_arr.value(0);
    let schema = try_schema_from_ipc_buffer(schema_bytes).expect("Invalid schema data");
    assert_eq!(
        schema.all_fields(),
        vec![
            &Field::new("city", DataType::Utf8, true),
            &Field::new("lat", DataType::Float64, true),
            &Field::new("lng", DataType::Float64, true),
        ]
    );

    // Get specific table with invalid table name filter
    let flight_info = client
        .get_tables(arrow_flight::sql::CommandGetTables {
            catalog: Some("roapi".to_string()),
            db_schema_filter_pattern: None,
            table_name_filter_pattern: Some("foo".to_string()),
            table_types: vec![],
            include_schema: true,
        })
        .await
        .unwrap();

    let batches = flight_info_to_batches(&mut client, flight_info).await;
    assert_eq!(batches.len(), 0);
}
