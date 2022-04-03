use connectorx::prelude::*;
use connectorx::sources::mysql::BinaryProtocol;
use datafusion::arrow::record_batch::RecordBatch;
use log::debug;

use crate::table::TableSource;

pub enum DatabaseLoader {
    MySQL,
    SQLite,
}

impl DatabaseLoader {
    pub async fn to_mem_table(
        &self,
        t: &TableSource,
    ) -> anyhow::Result<datafusion::datasource::MemTable> {
        debug!("loading database table data...");
        let queries = &[format!("SELECT * FROM {}", t.name)];
        let mut destination = ArrowDestination::new();
        match self {
            DatabaseLoader::MySQL => {
                let source = MySQLSource::<BinaryProtocol>::new(t.get_uri_str(), 2)?;
                let dispatcher = Dispatcher::<
                    MySQLSource<BinaryProtocol>,
                    ArrowDestination,
                    MySQLArrowTransport<BinaryProtocol>,
                >::new(source, &mut destination, queries, None);
                dispatcher.run()?;
            }
            DatabaseLoader::SQLite => {
                let uri = t.get_uri_str().replace("sqlite://", "");
                let source = SQLiteSource::new(&uri, 2)?;
                let dispatcher =
                    Dispatcher::<SQLiteSource, ArrowDestination, SQLiteArrowTransport>::new(
                        source,
                        &mut destination,
                        queries,
                        None,
                    );
                dispatcher.run()?;
            }
        };
        let schema_ref = destination.arrow_schema();
        let data: Vec<RecordBatch> = destination.arrow().unwrap();
        Ok(datafusion::datasource::MemTable::try_new(
            schema_ref.clone(),
            vec![data],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::datasource::TableProvider;
    use dotenv::dotenv;
    use std::env;

    use super::*;

    #[tokio::test]
    async fn load_mysql() -> anyhow::Result<()> {
        dotenv().ok();
        if let Ok(name) = env::var("TABLE_NAME") {
            let t = DatabaseLoader::MySQL
                .to_mem_table(&TableSource::new(name, env::var("MYSQL_URL")?))
                .await?;
            let stats = t.scan(&None, &[], None).await?.statistics();
            assert!(stats.num_rows.is_some());
        }

        Ok(())
    }
}
