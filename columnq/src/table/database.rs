pub enum DatabaseLoader {
    MySQL,
    SQLite,
    Postgres,
}

#[cfg(any(
    feature = "database-sqlite",
    feature = "database-mysql",
    feature = "database-postgres"
))]
mod imp {
    use crate::error::ColumnQError;
    use crate::table::TableSource;
    use connectorx::prelude::*;
    #[cfg(any(feature = "database-mysql"))]
    use connectorx::sources::mysql;
    #[cfg(any(feature = "database-postgres"))]
    use connectorx::sources::postgres;
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
                        let source = MySQLSource::<mysql::BinaryProtocol>::new(t.get_uri_str(), 2)
                            .map_err(|e| ColumnQError::Database(e.to_string()))?;
                        let dispatcher = Dispatcher::<
                            MySQLSource<mysql::BinaryProtocol>,
                            ArrowDestination,
                            MySQLArrowTransport<mysql::BinaryProtocol>,
                        >::new(
                            source, &mut destination, queries, None
                        );
                        dispatcher
                            .run()
                            .map_err(|e| ColumnQError::Database(e.to_string()))?;
                        let schema_ref = destination.arrow_schema();
                        let data: Vec<RecordBatch> = destination.arrow().unwrap();
                        Ok(datafusion::datasource::MemTable::try_new(
                            schema_ref,
                            vec![data],
                        )?)
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

                        let schema_ref = destination.arrow_schema();
                        let data: Vec<RecordBatch> = destination.arrow().unwrap();
                        Ok(datafusion::datasource::MemTable::try_new(
                            schema_ref,
                            vec![data],
                        )?)
                    }
                    #[cfg(not(feature = "database-sqlite"))]
                    {
                        return Err(ColumnQError::Database(
                            "SQLite database feature not enabled.".to_string(),
                        ));
                    }
                }
                DatabaseLoader::Postgres => {
                    #[cfg(feature = "database-postgres")]
                    {
                        use std::str::FromStr;
                        
                        let config = tokio_postgres::Config::from_str(t.get_uri_str())
                            .map_err(|e| ColumnQError::Database(e.to_string()))?;
                        let tls = match config.get_ssl_mode() {
                            tokio_postgres::config::SslMode::Require => tokio_postgres::NoTls,
                            _ => tokio_postgres::NoTls,
                        };
                        let source: PostgresSource<
                            postgres::BinaryProtocol,
                            tokio_postgres::NoTls,
                        > = PostgresSource::new(config.into(), tls, 2)
                            .map_err(|e| ColumnQError::Database(e.to_string()))?;
                        let queries = queries.clone();
                        let task = tokio::task::spawn_blocking(move || {
                            let dispatcher = Dispatcher::<
                                PostgresSource<postgres::BinaryProtocol, tokio_postgres::NoTls>,
                                ArrowDestination,
                                PostgresArrowTransport<
                                    postgres::BinaryProtocol,
                                    tokio_postgres::NoTls,
                                >,
                            >::new(
                                source, &mut destination, &queries, None
                            );

                            if let Err(e) = dispatcher.run() {
                                return Err(ColumnQError::Database(e.to_string()));
                            }

                            let schema_ref = destination.arrow_schema();
                            match destination.arrow() {
                                Ok(data) => datafusion::datasource::MemTable::try_new(
                                    schema_ref,
                                    vec![data],
                                )
                                .map_err(|e| ColumnQError::Database(e.to_string())),
                                Err(e) => Err(ColumnQError::Database(e.to_string())),
                            }
                        });

                        // FIXME: Maybe use other way to block the async task instead of block_on
                        futures::executor::block_on(task)
                            .map_err(|e| ColumnQError::Database(e.to_string()))?
                    }
                    #[cfg(not(feature = "database-postgres"))]
                    {
                        return Err(ColumnQError::Database(
                            "Postgres database feature not enabled.".to_string(),
                        ));
                    }
                }
            }
        }
    }
}

#[cfg(not(any(
    feature = "database-sqlite",
    feature = "database-mysql",
    feature = "database-postgres"
)))]
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
