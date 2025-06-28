mod helpers;

use columnq::datafusion::arrow::datatypes::DataType;
use columnq::table::{TableColumn, TableLoadOption, TableOptionCsv, TableSource};

fn partitioned_csv_table() -> TableSource {
    let table_path = helpers::test_data_path("partitioned_csv");
    TableSource::new("partitioned_csv".to_string(), table_path)
        .with_option(TableLoadOption::csv(
            TableOptionCsv::default().with_use_memory_table(false),
        ))
        .with_partition_columns(vec![
            TableColumn {
                name: "year".to_string(),
                data_type: DataType::UInt16,
                nullable: false,
            },
            TableColumn {
                name: "month".to_string(),
                data_type: DataType::UInt16,
                nullable: false,
            },
        ])
}

#[tokio::test]
async fn test_partitioned_csv_table() {
    let table = partitioned_csv_table();

    let (app, address) = helpers::test_api_app_with_tables(vec![table]).await;
    tokio::spawn(app.run_until_stopped());

    let response = helpers::http_post(
        &format!("{address}/api/sql"),
        "SELECT * FROM partitioned_csv ORDER BY ts ASC",
    )
    .await;

    let status = response.status();
    let data = response.json::<serde_json::Value>().await.unwrap();
    assert_eq!(
        data,
        serde_json::json!([
            {"year": 2022, "month": 12, "ts": 100, "value": 0.5},
            {"year": 2022, "month": 12, "ts": 101, "value": 7.8},
            {"year": 2022, "month": 12, "ts": 102, "value": 4.0},
            {"year": 2023, "month": 1, "ts": 201, "value": -1.0},
            {"year": 2023, "month": 1, "ts": 202, "value": 100.0},
            {"year": 2023, "month": 1, "ts": 203, "value": 0.0},
        ])
    );
    assert_eq!(status, 200);
}

#[tokio::test]
async fn test_partitioned_csv_table_filter_by_partition() {
    let table = partitioned_csv_table();

    let (app, address) = helpers::test_api_app_with_tables(vec![table]).await;
    tokio::spawn(app.run_until_stopped());

    let response = helpers::http_post(
        &format!("{address}/api/sql"),
        "SELECT * FROM partitioned_csv WHERE year = '2022' AND month = 12 AND value > 1",
    )
    .await;

    let status = response.status();
    let data = response.json::<serde_json::Value>().await.unwrap();
    assert_eq!(
        data,
        serde_json::json!([
            {"year": 2022, "month": 12, "ts": 101, "value": 7.8},
            {"year": 2022, "month": 12, "ts": 102, "value": 4.0},
        ])
    );
    assert_eq!(status, 200);
}
