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
    use crate::table::TableLoadOption;
    use crate::table::{self, TableSource};
    use connectorx::prelude::*;
    use log::debug;
    use snafu::prelude::*;

    use super::DatabaseLoader;

    #[derive(Debug, Snafu)]
    pub enum Error {
        #[snafu(display("Failed to create source connection: {source}"))]
        Source {
            source: connectorx::prelude::ConnectorXError,
        },
        #[snafu(display("Failed to create destination connection: {source}"))]
        Destination {
            source: connectorx::prelude::ConnectorXOutError,
        },
        #[snafu(display("Failed to convert to arrow: {source}"))]
        ToArrow {
            source: connectorx::destinations::arrow::ArrowDestinationError,
        },
    }

    impl DatabaseLoader {
        pub fn to_mem_table(
            &self,
            t: &TableSource,
        ) -> Result<datafusion::datasource::MemTable, table::Error> {
            debug!("loading database table data...");

            let table_name = match &t.option {
                Some(TableLoadOption::mysql { table }) => table.clone(),
                Some(TableLoadOption::postgres { table }) => table.clone(),
                Some(TableLoadOption::sqlite { table }) => table.clone(),
                _ => None,
            }
            .unwrap_or(t.name.clone());

            let queries = CXQuery::naked(format!("SELECT * FROM {table_name}"));
            let source = SourceConn::try_from(t.get_uri_str())
                .context(SourceSnafu)
                .map_err(Box::new)
                .context(table::LoadDatabaseSnafu)?;
            let destination = connectorx::get_arrow::get_arrow(&source, None, &[queries])
                .context(DestinationSnafu)
                .map_err(Box::new)
                .context(table::LoadDatabaseSnafu)?;

            datafusion::datasource::MemTable::try_new(
                destination.arrow_schema(),
                vec![destination
                    .arrow()
                    .context(ToArrowSnafu)
                    .map_err(Box::new)
                    .context(table::LoadDatabaseSnafu)?],
            )
            .map_err(Box::new)
            .context(table::CreateMemTableSnafu)
        }
    }
}

#[cfg(not(any(
    feature = "database-sqlite",
    feature = "database-mysql",
    feature = "database-postgres"
)))]
mod imp {
    use crate::table::TableSource;

    use super::DatabaseLoader;
    use crate::table;
    use snafu::prelude::*;

    #[derive(Debug, Snafu)]
    pub struct Error {}

    impl DatabaseLoader {
        pub fn to_mem_table(
            &self,
            _t: &TableSource,
        ) -> Result<datafusion::datasource::MemTable, table::Error> {
            Err(table::Error::Generic {
                msg: "Enable 'database' feature flag to support this".to_string(),
            })
        }
    }
}

pub use imp::Error;
