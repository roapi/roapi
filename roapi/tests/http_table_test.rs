mod helpers;

use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;

use axum::http::HeaderMap;
use axum::{extract::State, response::IntoResponse, routing::get};

async fn http_server() -> (
    axum::Server<hyper::server::conn::AddrIncoming, axum::routing::IntoMakeService<axum::Router>>,
    std::net::SocketAddr,
) {
    async fn serve_json(
        State(response_str): State<Arc<String>>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        assert_eq!(headers.get("X-FOO").expect("X-FOO header not found"), "bar");
        response_str.to_string().into_response()
    }

    let json_source_path = helpers::test_data_path("spacex_launches.json");
    let response_str = tokio::fs::read_to_string(json_source_path)
        .await
        .expect("Failed to read JSON file");
    let state = Arc::new(response_str);

    let app = axum::Router::new()
        .route("/table.json", get(serve_json))
        .with_state(state);

    let listener = TcpListener::bind("localhost:0").unwrap();
    let addr = listener
        .local_addr()
        .expect("Failed to get address from listener");

    let http_server = axum::Server::from_tcp(listener).unwrap();
    let http_server = http_server.serve(app.into_make_service());

    (http_server, addr)
}

#[tokio::test]
async fn test_http_backed_spacex_table() {
    let (http_server, http_addr) = http_server().await;

    tokio::spawn(async move { http_server.await.expect("Failed to start HTTP server") });

    let mut json_http_table = columnq::table::TableSource::new(
        "spacex_launches".to_string(),
        format!("http://localhost:{}/table.json", http_addr.port()),
    );
    let mut headers: HashMap<String, String> = HashMap::new();
    headers.insert("X-FOO".to_string(), "bar".to_string());
    json_http_table.io_option = Some(columnq::io::IoOption::http {
        headers: Some(headers),
    });
    let (app, address) = helpers::test_api_app_with_tables(vec![json_http_table]).await;
    tokio::spawn(app.run_until_stopped());

    let response = helpers::http_post(
        &format!("{address}/api/sql"),
        "SELECT COUNT(*) as c FROM spacex_launches",
    )
    .await;

    assert_eq!(response.status(), 200);
    let data = response.json::<serde_json::Value>().await.unwrap();
    assert_eq!(
        data,
        serde_json::json!([
            {"c": 132},
        ])
    );
}
