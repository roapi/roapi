mod helpers;

use anyhow::Result;

use tokio_postgres::NoTls;

#[tokio::test]
async fn test_postgres_count() -> Result<()> {
    let json_table = helpers::get_spacex_table();
    let (app, _) = helpers::test_api_app_with_tables(vec![json_table]).await;
    let addr = app.postgres_addr();
    tokio::spawn(app.run_until_stopped());

    let conn_str = format!("host={} port={}", addr.ip(), addr.port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    let rows = client
        .simple_query("SELECT COUNT(*) FROM spacex_launches")
        .await?;

    match &rows[0] {
        tokio_postgres::SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get(0).unwrap(), "132");
        }
        _ => {
            panic!("expect row from query result.");
        }
    }

    match &rows[1] {
        tokio_postgres::SimpleQueryMessage::CommandComplete(modified) => {
            assert_eq!(modified, &1);
        }
        _ => {
            panic!("expect command complete from query result.");
        }
    }

    assert_eq!(rows.len(), 2);

    Ok(())
}
