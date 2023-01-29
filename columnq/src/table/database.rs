pub enum DatabaseLoader {
    MySQL,
    SQLite,
    Postgres,
}

#[cfg(any(feature = "database-sqlite", feature = "database-mysql"))]
mod imp {
    use crate::error::ColumnQError;
    use crate::table::TableSource;
    use connectorx::prelude::*;
    #[cfg(feature = "database-mysql")]
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
                    #[cfg(feature = "database-mysql")]
                    {
                        let source = MySQLSource::<BinaryProtocol>::new(t.get_uri_str(), 2)
                            .map_err(|e| ColumnQError::Database(e.to_string()))?;
                        let dispatcher = Dispatcher::<
                            MySQLSource<BinaryProtocol>,
                            ArrowDestination,
                            MySQLArrowTransport<BinaryProtocol>,
                        >::new(
                            source, &mut destination, queries, None
                        );
                        dispatcher
                            .run()
                            .map_err(|e| ColumnQError::Database(e.to_string()))?;
                    }
                    #[cfg(not(feature = "database-mysql"))]
                    {
                        return Err(ColumnQError::Database(
                            "MySQL database feature not enabled.".to_string(),
                        ));
                    }
                }
                DatabaseLoader::SQLite => {
                    #[cfg(feature = "database-sqlite")]
                    {
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
                    #[cfg(not(feature = "database-sqlite"))]
                    {
                        return Err(ColumnQError::Database(
                            "SQLite database feature not enabled.".to_string(),
                        ));
                    }
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

#[cfg(not(any(feature = "database-sqlite", feature = "database-mysql")))]
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
