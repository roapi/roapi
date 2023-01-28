#[cfg(any(feature = "database-mysql"))]
mod mysql {
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;
    use std::env;

    use columnq::table::TableSource;

    use columnq::table::database::DatabaseLoader;

    #[tokio::test]
    async fn load_mysql() -> anyhow::Result<()> {
        dotenvy::dotenv().ok();
        if let Ok(name) = env::var("TABLE_NAME") {
            let t = DatabaseLoader::MySQL
                .to_mem_table(&TableSource::new(name, env::var("MYSQL_URL")?))?;
            let ctx = SessionContext::new();
            let stats = t.scan(&ctx.state(), &None, &[], None).await?.statistics();
            assert!(stats.num_rows.is_some());
        }

        Ok(())
    }
}
