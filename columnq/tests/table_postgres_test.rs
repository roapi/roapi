#[cfg(feature = "database-postgres")]
mod postgres {
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;
    use std::env;

    use columnq::table::TableSource;

    use columnq::table::database::DatabaseLoader;

    #[tokio::test]
    async fn load_postgres() {
        dotenvy::dotenv().ok();
        if let Ok(name) = env::var("TABLE_NAME") {
            let t = DatabaseLoader::Postgres
                .to_mem_table(&TableSource::new(name, env::var("POSGRES_URL").unwrap()))
                .unwrap();
            let ctx = SessionContext::new();
            let stats = t
                .scan(&ctx.state(), None, &[], None)
                .await
                .unwrap()
                .statistics()
                .unwrap();
            assert!(stats.num_rows.get_value().is_some());
        }
    }
}
