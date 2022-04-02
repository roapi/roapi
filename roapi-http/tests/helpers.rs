use std::path::PathBuf;

use columnq::datafusion::arrow;
use columnq::table::{KeyValueSource, TableColumn, TableLoadOption, TableSchema, TableSource};
use roapi_http::config::Config;
use roapi_http::startup::Application;

pub fn test_data_path(relative_path: &str) -> String {
    let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("../test_data");
    d.push(relative_path);
    d.to_string_lossy().to_string()
}

pub async fn test_api_app_with_tables(tables: Vec<TableSource>) -> (Application, String) {
    test_api_app(tables, vec![]).await
}

pub async fn test_api_app_with_kvstores(kvstores: Vec<KeyValueSource>) -> (Application, String) {
    test_api_app(vec![], kvstores).await
}

pub async fn test_api_app(
    tables: Vec<TableSource>,
    kvstores: Vec<KeyValueSource>,
) -> (Application, String) {
    let config = Config {
        addr: "localhost:0".to_string().into(),
        tables,
        disable_read_only: false,
        kvstores,
    };

    let app = Application::build(config)
        .await
        .expect("Failed to build application config");
    let port = app.port();
    let address = format!("http://localhost:{}", port);

    (app, address)
}

pub async fn http_get(url: &str, accept: Option<&str>) -> reqwest::Response {
    let request = reqwest::Client::new().get(url);
    let request = if let Some(accept) = accept {
        request.header("Accept", accept)
    } else {
        request
    };
    request.send().await.expect("Unable to execute GET request")
}

pub async fn http_post(url: &str, payload: impl Into<reqwest::Body>) -> reqwest::Response {
    reqwest::Client::new()
        .post(url)
        .body(payload)
        .send()
        .await
        .expect("Unable to execute POST request")
}

pub fn get_spacex_table() -> TableSource {
    let json_source_path = test_data_path("spacex_launches.json");
    TableSource::new("spacex_launches".to_string(), json_source_path)
}

pub fn get_uk_cities_table() -> TableSource {
    TableSource::new(
        "uk_cities".to_string(),
        test_data_path("uk_cities_with_headers.csv"),
    )
}

pub fn get_ubuntu_ami_table() -> TableSource {
    TableSource::new("ubuntu_ami", test_data_path("ubuntu-ami.json"))
        .with_option(TableLoadOption::json {
            pointer: Some("/aaData".to_string()),
            array_encoded: Some(true),
        })
        .with_schema(TableSchema {
            columns: vec![
                TableColumn {
                    name: "zone".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: true,
                },
                TableColumn {
                    name: "name".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: true,
                },
                TableColumn {
                    name: "version".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: true,
                },
                TableColumn {
                    name: "arch".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: true,
                },
                TableColumn {
                    name: "instance_type".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: true,
                },
                TableColumn {
                    name: "release".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: true,
                },
                TableColumn {
                    name: "ami_id".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: true,
                },
                TableColumn {
                    name: "aki_id".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: true,
                },
            ],
        })
}

pub fn get_spacex_launch_name_kvstore() -> KeyValueSource {
    KeyValueSource::new(
        "spacex_launch_name",
        test_data_path("spacex_launches.json"),
        "id",
        "name",
    )
}
