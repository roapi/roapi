mod helpers;

use std::collections::HashMap;

use anyhow::Result;
use async_process::Command;
use columnq::arrow::datatypes::Schema;

#[tokio::test]
async fn test_schema() -> Result<()> {
    let json_table = helpers::get_spacex_table();
    let (app, address) = helpers::test_api_app_with_tables(vec![json_table]).await;
    tokio::spawn(app.run_until_stopped());

    let response = helpers::http_get(&format!("{address}/api/schema"), None).await;

    assert_eq!(response.status(), 200);
    let body = response.json::<HashMap<String, Schema>>().await?;
    assert!(body.contains_key("spacex_launches"));
    Ok(())
}

#[tokio::test]
async fn test_uk_cities_sql_post() -> Result<()> {
    let table = helpers::get_uk_cities_table();
    let (app, address) = helpers::test_api_app_with_tables(vec![table]).await;
    tokio::spawn(app.run_until_stopped());

    let response = helpers::http_post(
        &format!("{address}/api/sql"),
        "SELECT city FROM uk_cities WHERE lat > 52 and lat < 53 and lng < -1",
    )
    .await;

    assert_eq!(response.status(), 200);
    let data = response.json::<serde_json::Value>().await?;
    assert_eq!(
        data,
        serde_json::json!([
            {"city": "Solihull, Birmingham, UK"},
            {"city": "Rugby, Warwickshire, UK"},
            {"city": "Sutton Coldfield, West Midlands, UK"},
            {"city": "Wolverhampton, West Midlands, UK"},
            {"city": "Frankton, Warwickshire, UK"}
        ])
    );
    Ok(())
}

#[tokio::test]
async fn test_sql_invalid_post() -> Result<()> {
    let table = helpers::get_uk_cities_table();
    let (app, address) = helpers::test_api_app_with_tables(vec![table]).await;
    tokio::spawn(app.run_until_stopped());

    let response = helpers::http_post(&format!("{address}/api/sql"), "SELECT city FROM").await;

    assert_eq!(response.status(), 400);
    let data = response.json::<serde_json::Value>().await?;
    assert_eq!(
        data,
        serde_json::json!({
            "code": 400,
            "error": "plan_sql",
            "message": "Failed to plan execution from SQL query: SQL error: ParserError(\"Expected identifier, found: EOF\")"
        })
    );
    Ok(())
}

#[tokio::test]
async fn test_ubuntu_ami_sql_post() -> Result<()> {
    let table = helpers::get_ubuntu_ami_table();
    let (app, address) = helpers::test_api_app_with_tables(vec![table]).await;
    tokio::spawn(app.run_until_stopped());

    let response = helpers::http_post(
        &format!("{address}/api/sql"),
        "SELECT ami_id FROM ubuntu_ami \
                WHERE version='12.04 LTS' \
                    AND arch = 'amd64' \
                    AND zone='us-west-2' \
                    AND instance_type='hvm:ebs-ssd'",
    )
    .await;

    assert_eq!(response.status(), 200);
    let data = response.json::<serde_json::Value>().await?;
    assert_eq!(
        data,
        serde_json::json!([
            {"ami_id":"<a href=\"https://console.aws.amazon.com/ec2/home?region=us-west-2#launchAmi=ami-270f9747\">ami-270f9747</a>"}
        ])
    );
    Ok(())
}

#[tokio::test]
async fn test_rest_get() -> Result<()> {
    let table = helpers::get_ubuntu_ami_table();
    let (app, address) = helpers::test_api_app_with_tables(vec![table]).await;
    tokio::spawn(app.run_until_stopped());
    let accept_headers = vec![
        None,
        Some("application/json"),
        Some("text/html,application/xhtml+xml,application/xml;q=0.9"),
        Some("text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.5"),
    ];

    for accept_header in accept_headers {
        let response = helpers::http_get(
            &format!(
                "{address}/api/tables/ubuntu_ami?\
                columns=name,version,release&\
                filter[arch]='amd64'&\
                filter[zone]eq='us-west-2'&\
                filter[instance_type]eq='hvm:ebs-ssd'&\
                sort=-version,release\
                "
            ),
            accept_header,
        )
        .await;

        assert_eq!(response.status(), 200);
        let data = response.json::<serde_json::Value>().await?;
        assert_eq!(
            data,
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
    Ok(())
}

#[tokio::test]
async fn test_graphql_post_query_op() -> Result<()> {
    let table = helpers::get_ubuntu_ami_table();
    let (app, address) = helpers::test_api_app_with_tables(vec![table]).await;
    tokio::spawn(app.run_until_stopped());

    let response = helpers::http_post(
        &format!("{address}/api/graphql"),
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
    .await;

    assert_eq!(response.status(), 200);
    let data = response.json::<serde_json::Value>().await?;
    assert_eq!(
        data,
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
    Ok(())
}

#[tokio::test]
async fn test_graphql_post_selection() -> Result<()> {
    let table = helpers::get_ubuntu_ami_table();
    let (app, address) = helpers::test_api_app_with_tables(vec![table]).await;
    tokio::spawn(app.run_until_stopped());

    let response = helpers::http_post(
        &format!("{address}/api/graphql"),
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
    .await;

    assert_eq!(response.status(), 200);
    let data = response.json::<serde_json::Value>().await?;
    assert_eq!(
        data,
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
    Ok(())
}

#[tokio::test]
async fn test_http2() -> Result<()> {
    let table = helpers::get_uk_cities_table();
    let (app, address) = helpers::test_api_app_with_tables(vec![table]).await;
    tokio::spawn(app.run_until_stopped());

    // ~ % curl -sI --http2-prior-knowledge localhost:8080/api/schema -o/dev/null -w '%{http_version}\n'
    // 2

    let http_version = Command::new("curl")
        .arg("-s")
        .arg("-I")
        .arg("--http2-prior-knowledge")
        .arg(format!("{address}/api/schema"))
        .arg("-o")
        .arg("/dev/null")
        .arg("-w")
        .arg("'%{http_version}\n'")
        .output()
        .await?
        .stdout;

    let two = vec!['\'', '2', '\n', '\'']
        .iter()
        .map(|c| *c as u8)
        .collect::<Vec<_>>();

    assert_eq!(http_version, two);
    Ok(())
}

#[tokio::test]
async fn test_kvstore_get() -> Result<()> {
    let store = helpers::get_spacex_launch_name_kvstore();
    let (app, address) = helpers::test_api_app_with_kvstores(vec![store]).await;
    tokio::spawn(app.run_until_stopped());

    let response = helpers::http_get(
        &format!("{address}/api/kv/spacex_launch_name/600f9a8d8f798e2a4d5f979e"),
        None,
    )
    .await;

    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await?, "Starlink-21 (v1.0)");
    Ok(())
}
