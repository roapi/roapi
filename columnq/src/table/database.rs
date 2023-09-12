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
    use std::convert::TryFrom;

    use crate::error::ColumnQError;
    use crate::table::TableSource;
    use connectorx::prelude::*;
    use log::debug;

    use super::DatabaseLoader;

    impl DatabaseLoader {
        pub fn to_mem_table(
            &self,
            t: &TableSource,
        ) -> Result<datafusion::datasource::MemTable, ColumnQError> {
            debug!("loading database table data...");
            let queries = CXQuery::naked(format!("SELECT * FROM {}", t.name));
            let source = SourceConn::try_from(t.get_uri_str())
                .map_err(|e| ColumnQError::Database(e.to_string()))?;
            let destination = connectorx::get_arrow::get_arrow(&source, None, &[queries])
                .map_err(|e| ColumnQError::Database(e.to_string()))?;
            Ok(datafusion::datasource::MemTable::try_new(
                destination.arrow_schema(),
                vec![destination
                    .arrow()
                    .map_err(|e| ColumnQError::Database(e.to_string()))?],
            )?)
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
