#![deny(warnings)]

#[macro_use]
extern crate lazy_static;

pub mod error;

macro_rules! partitions_from_table_source {
    ($table_source:ident, $call_with_r:expr) => {{
        use crate::io;
        use std::convert::TryFrom;

        let uri = $table_source.parsed_uri()?;
        match io::BlobStoreType::try_from(uri.scheme())? {
            io::BlobStoreType::FileSystem => {
                io::fs::partitions_from_uri(&$table_source, uri, $call_with_r)
            }
            io::BlobStoreType::Http => {
                io::http::partitions_from_uri(&$table_source, uri, $call_with_r).await
            }
            io::BlobStoreType::S3 | io::BlobStoreType::GCS | io::BlobStoreType::Azure => {
                io::object_store::partitions_from_uri(&$table_source, uri, $call_with_r).await
            }
            io::BlobStoreType::Memory => {
                io::memory::partitions_from_memory(&$table_source, $call_with_r).await
            }
        }
    }};
}

pub mod columnq;
pub mod encoding;
pub mod io;
pub mod query;
pub mod table;

pub use crate::columnq::*;

/// export datafusion and arrow so downstream won't need to declare dependencies on these libraries
pub use datafusion;
pub use datafusion::arrow;
pub use sqlparser;

#[cfg(test)]
pub mod test_util;
