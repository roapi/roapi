use actix_http::body::MessageBody;
use actix_service::ServiceFactory;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::{web, App, Error};

use crate::api;

pub fn register_app_routes<T, B>(app: App<T, B>) -> App<T, B>
where
    B: MessageBody,
    T: ServiceFactory<
        ServiceRequest,
        Config = (),
        Response = ServiceResponse<B>,
        Error = Error,
        InitError = (),
    >,
{
    app.route(
        "/api/tables/{table_name}",
        web::get().to(api::rest::get_table),
    )
    .route("/api/sql", web::post().to(api::sql::post))
    .route("/api/graphql", web::post().to(api::graphql::post))
    .route("/api/schema", web::get().to(api::schema::get))
    .route(
        "/api/schema/{table_name}",
        web::get().to(api::schema::get_by_table_name),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use actix_web::{http, test, web, App};
    use columnq::table::*;

    use crate::api::HandlerContext;
    use crate::config::Config;
    use crate::test_util::*;

    async fn test_handler_context_uk_cities() -> web::Data<HandlerContext> {
        web::Data::new(
            HandlerContext::new(&Config {
                addr: None,
                tables: vec![TableSource {
                    name: "uk_cities".to_string(),
                    uri: test_data_path("uk_cities_with_headers.csv"),
                    schema: None,
                    option: None,
                }],
            })
            .await
            .unwrap(),
        )
    }

    async fn test_handler_context_ubuntu_ami() -> web::Data<HandlerContext> {
        web::Data::new(
            HandlerContext::new(&Config {
                addr: None,
                tables: vec![TableSource {
                    name: "ubuntu_ami".to_string(),
                    uri: test_data_path("ubuntu-ami.json"),
                    option: Some(TableLoadOption::json {
                        pointer: Some("/aaData".to_string()),
                        array_encoded: Some(true),
                    }),
                    schema: Some(TableSchema {
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
                    }),
                }],
            })
            .await
            .unwrap(),
        )
    }

    #[actix_rt::test]
    async fn api_schema() {
        let ctx = test_handler_context_uk_cities().await;
        let mut app = test::init_service(register_app_routes(App::new().app_data(ctx))).await;

        let req = test::TestRequest::with_uri("/api/schema").to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
    }

    #[actix_rt::test]
    async fn api_sql_post() {
        let ctx = test_handler_context_uk_cities().await;
        let mut app = test::init_service(register_app_routes(App::new().app_data(ctx))).await;

        let req = test::TestRequest::post()
            .uri("/api/sql")
            .set_payload("SELECT city FROM uk_cities WHERE lat > 52 and lat < 53 and lng < -1")
            .to_request();
        let mut resp = test::call_service(&mut app, req).await;
        match &resp.take_body() {
            actix_web::body::ResponseBody::Body(actix_web::body::Body::Bytes(b)) => {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&b[..]).unwrap(),
                    serde_json::json!([
                        {"city": "Solihull, Birmingham, UK"},
                        {"city": "Rugby, Warwickshire, UK"},
                        {"city": "Sutton Coldfield, West Midlands, UK"},
                        {"city": "Wolverhampton, West Midlands, UK"},
                        {"city": "Frankton, Warwickshire, UK"}
                    ])
                );
            }
            _ => panic!("invalid response body"),
        }
        assert_eq!(resp.status(), http::StatusCode::OK);
    }

    #[actix_rt::test]
    async fn api_sql_invalid_post() {
        let ctx = test_handler_context_uk_cities().await;
        let mut app = test::init_service(register_app_routes(App::new().app_data(ctx))).await;

        let req = test::TestRequest::post()
            .uri("/api/sql")
            .set_payload("SELECT city FROM")
            .to_request();
        let mut resp = test::call_service(&mut app, req).await;
        match &resp.take_body() {
            actix_web::body::ResponseBody::Body(actix_web::body::Body::Bytes(b)) => {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&b[..]).unwrap(),
                    serde_json::json!({
                        "code": 400,
                        "error": "plan_sql",
                        "message": "Failed to plan execution from SQL query: SQL error: ParserError(\"Expected identifier, found: EOF\")"
                    })
                );
            }
            _ => panic!("invalid response body"),
        }
        assert_eq!(resp.status(), http::StatusCode::BAD_REQUEST);
    }

    #[actix_rt::test]
    async fn api_sql_post_json() {
        let ctx = test_handler_context_ubuntu_ami().await;
        let mut app = test::init_service(register_app_routes(App::new().app_data(ctx))).await;

        let req = test::TestRequest::post()
            .uri("/api/sql")
            .set_payload(
                "SELECT ami_id FROM ubuntu_ami \
                WHERE version='12.04 LTS' \
                    AND arch = 'amd64' \
                    AND zone='us-west-2' \
                    AND instance_type='hvm:ebs-ssd'",
            )
            .to_request();
        let mut resp = test::call_service(&mut app, req).await;
        match &resp.take_body() {
            actix_web::body::ResponseBody::Body(actix_web::body::Body::Bytes(b)) => {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&b[..]).unwrap(),
                    serde_json::json!([
                        {"ami_id":"<a href=\"https://console.aws.amazon.com/ec2/home?region=us-west-2#launchAmi=ami-270f9747\">ami-270f9747</a>"}
                    ])
                );
            }
            _ => panic!("invalid response body"),
        }
        assert_eq!(resp.status(), http::StatusCode::OK);
    }

    #[actix_rt::test]
    async fn api_rest_get() {
        let ctx = test_handler_context_ubuntu_ami().await;
        let mut app = test::init_service(register_app_routes(App::new().app_data(ctx))).await;

        let req = test::TestRequest::get()
            .uri(
                "/api/tables/ubuntu_ami?\
                columns=name,version,release&\
                filter[arch]='amd64'&\
                filter[zone]eq='us-west-2'&\
                filter[instance_type]eq='hvm:ebs-ssd'&\
                sort=-version,release\
                ",
            )
            .to_request();
        let mut resp = test::call_service(&mut app, req).await;
        match &resp.take_body() {
            actix_web::body::ResponseBody::Body(actix_web::body::Body::Bytes(b)) => {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&b[..]).unwrap(),
                    serde_json::json!([
                        { "release": "20201205", "version": "20.10", "name": "groovy" },
                        { "release": "20201201", "version": "20.04 LTS", "name": "focal" },
                        { "release": "20200716.1", "version": "19.10", "name": "eoan" },
                        { "release": "20200115", "version": "19.04", "name": "disco" },
                        { "release": "20201201", "version": "18.04 LTS", "name": "bionic" },
                        { "release": "20201202.1", "version": "16.04 LTS", "name": "xenial" },
                        { "release": "20191107", "version": "14.04 LTS", "name": "trusty" },
                        { "release": "20170502", "version": "12.04 LTS", "name": "precise" }
                    ])
                );
            }
            _ => panic!("invalid response body"),
        }
        assert_eq!(resp.status(), http::StatusCode::OK);
    }

    #[actix_rt::test]
    async fn api_graphql_post_query_op() {
        let ctx = test_handler_context_ubuntu_ami().await;
        let mut app = test::init_service(register_app_routes(App::new().app_data(ctx))).await;

        let req = test::TestRequest::post()
            .uri("/api/graphql")
            .set_payload(
                r#"query {
                    ubuntu_ami(
                        filter: {
                            arch: "amd64"
                            zone: { eq: "us-west-2" }
                            instance_type: { eq: "hvm:ebs-ssd" }
                        }
                        sort: [
                            { field: "version", order: "desc" }
                            { field: "release" }
                        ]
                    ) {
                        name
                        version
                        release
                    }
                }"#,
            )
            .to_request();
        let mut resp = test::call_service(&mut app, req).await;
        match &resp.take_body() {
            actix_web::body::ResponseBody::Body(actix_web::body::Body::Bytes(b)) => {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&b[..]).unwrap(),
                    serde_json::json!([
                        { "release": "20201205", "version": "20.10", "name": "groovy" },
                        { "release": "20201201", "version": "20.04 LTS", "name": "focal" },
                        { "release": "20200716.1", "version": "19.10", "name": "eoan" },
                        { "release": "20200115", "version": "19.04", "name": "disco" },
                        { "release": "20201201", "version": "18.04 LTS", "name": "bionic" },
                        { "release": "20201202.1", "version": "16.04 LTS", "name": "xenial" },
                        { "release": "20191107", "version": "14.04 LTS", "name": "trusty" },
                        { "release": "20170502", "version": "12.04 LTS", "name": "precise" }
                    ])
                );
            }
            _ => panic!("invalid response body"),
        }
        assert_eq!(resp.status(), http::StatusCode::OK);
    }

    #[actix_rt::test]
    async fn api_graphql_post_selection() {
        let ctx = test_handler_context_ubuntu_ami().await;
        let mut app = test::init_service(register_app_routes(App::new().app_data(ctx))).await;

        let req = test::TestRequest::post()
            .uri("/api/graphql")
            .set_payload(
                r#"{
                ubuntu_ami(
                    filter: {
                        arch: "amd64"
                        zone: { eq: "us-west-2" }
                        instance_type: { eq: "hvm:ebs-ssd" }
                    }
                    sort: [
                        { field: "version", order: "desc" }
                    ]
                ) {
                    name
                    version
                }
            }"#,
            )
            .to_request();
        let mut resp = test::call_service(&mut app, req).await;
        match &resp.take_body() {
            actix_web::body::ResponseBody::Body(actix_web::body::Body::Bytes(b)) => {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&b[..]).unwrap(),
                    serde_json::json!([
                        { "version": "20.10", "name": "groovy" },
                        { "version": "20.04 LTS", "name": "focal" },
                        { "version": "19.10", "name": "eoan" },
                        { "version": "19.04", "name": "disco" },
                        { "version": "18.04 LTS", "name": "bionic" },
                        { "version": "16.04 LTS", "name": "xenial" },
                        { "version": "14.04 LTS", "name": "trusty" },
                        { "version": "12.04 LTS", "name": "precise" }
                    ])
                );
            }
            _ => panic!("invalid response body"),
        }
        assert_eq!(resp.status(), http::StatusCode::OK);
    }
}
