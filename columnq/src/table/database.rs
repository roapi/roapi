pub enum DatabaseLoader {
    MySQL,
    SQLite,
    Postgres,
}

#[cfg(feature = "database")]
mod imp {
    use crate::error::ColumnQError;
    use crate::table::TableSource;
    use connectorx::prelude::*;
    use connectorx::sources::mysql::BinaryProtocol;
    use datafusion::arrow::record_batch::RecordBatch;
    use log::debug;

    use super::DatabaseLoader;

    impl DatabaseLoader {
        pub fn to_mem_table(
            &self,
            t: &TableSource,
        ) -> Result<datafusion::datasource::MemTable, ColumnQError> {
            debug!("loading database table data...");
            let queries = &[format!("SELECT * FROM {}", t.name)];
            let mut destination = ArrowDestination::new();
            match self {
                DatabaseLoader::MySQL => {
                    let source = MySQLSource::<BinaryProtocol>::new(t.get_uri_str(), 2)
                        .map_err(|e| ColumnQError::Database(e.to_string()))?;
                    let dispatcher =
                        Dispatcher::<
                            MySQLSource<BinaryProtocol>,
                            ArrowDestination,
                            MySQLArrowTransport<BinaryProtocol>,
                        >::new(source, &mut destination, queries, None);
                    dispatcher
                        .run()
                        .map_err(|e| ColumnQError::Database(e.to_string()))?;
                }
                DatabaseLoader::SQLite => {
                    let uri = t.get_uri_str().replace("sqlite://", "");
                    let source = SQLiteSource::new(&uri, 2)
                        .map_err(|e| ColumnQError::Database(e.to_string()))?;
                    let dispatcher = Dispatcher::<
                        SQLiteSource,
                        ArrowDestination,
                        SQLiteArrowTransport,
                    >::new(
                        source, &mut destination, queries, None
                    );
                    dispatcher
                        .run()
                        .map_err(|e| ColumnQError::Database(e.to_string()))?;
                }
                DatabaseLoader::Postgres => {
                    // ToDo `Cannot start a runtime from within a runtime` error in `connector-x PostgresSource`
                    return Err(ColumnQError::Database(
                        "Postgres database features not be supported for now.".to_string(),
                    ));
                }
            };
            let schema_ref = destination.arrow_schema();
            let data: Vec<RecordBatch> = destination.arrow().unwrap();
            Ok(datafusion::datasource::MemTable::try_new(
                schema_ref,
                vec![data],
            )?)
        }
    }
}

#[cfg(not(feature = "database"))]
mod imp {
    use crate::error::ColumnQError;
    use crate::table::TableSource;

    use super::DatabaseLoader;

    impl DatabaseLoader {
        pub fn to_mem_table(
            &self,
            _t: &TableSource,
        ) -> Result<datafusion::datasource::MemTable, ColumnQError> {
            Err(ColumnQError::Database(
                "Enable 'database' feature flag to support this".to_string(),
            ))
        }
    }
}

pub use imp::*;

#[cfg(feature = "database")]
#[cfg(test)]
mod tests {
    use datafusion::datasource::TableProvider;
    use dotenv::dotenv;
    use std::env;

    use crate::table::TableSource;

    use super::*;

    #[tokio::test]
    async fn load_mysql() -> anyhow::Result<()> {
        dotenv().ok();
        if let Ok(name) = env::var("TABLE_NAME") {
            let t = DatabaseLoader::MySQL
                .to_mem_table(&TableSource::new(name, env::var("MYSQL_URL")?))?;
            let stats = t.scan(&None, &[], None).await?.statistics();
            assert!(stats.num_rows.is_some());
        }

        Ok(())
    }
}
