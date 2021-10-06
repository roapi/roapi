mod helpers;

use anyhow::Result;

use crate::helpers::test_data_path;
use columnq::arrow::datatypes::Schema;
use columnq::table::TableSource;
use roapi_http::config::Config;
use roapi_http::startup::Application;
use std::collections::HashMap;

#[actix_rt::test]
async fn test_schema() -> Result<()> {
    let json_source_path = test_data_path("spacex_launches.json");
    let json_table = TableSource::new("spacex_launches".to_string(), json_source_path);
    let config = Config {
        addr: "localhost:0".to_string().into(),
        tables: vec![json_table],
    };

    let application = Application::build(config)
        .await
        .expect("Failed to build application config");
    let port = application.port();
    let address = format!("http://localhost:{}", port);

    tokio::spawn(application.run_until_stopped());

    let response = reqwest::Client::new()
        .get(&format!("{}/api/schema", address))
        .send()
        .await
        .expect("Unable to execute request");

    assert_eq!(response.status(), 200);
    let body = response.json::<HashMap<String, Schema>>().await?;
    assert!(body.contains_key("spacex_launches"));

    Ok(())
}
