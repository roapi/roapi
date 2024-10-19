#![deny(warnings)]

pub mod error;

macro_rules! partitions_from_table_source {
    ($table_source:ident, $call_with_r:expr, $ctx:ident) => {{
        use crate::io;
        use std::convert::TryFrom;

        let uri = $table_source.parsed_uri()?;
        match io::BlobStoreType::try_from(uri.scheme()).context(table::IoSnafu)? {
            io::BlobStoreType::FileSystem => {
                io::fs::partitions_from_uri(&$table_source, uri, $call_with_r)
            }
            io::BlobStoreType::Http => {
                io::http::partitions_from_uri(&$table_source, uri, $call_with_r).await
            }
            io::BlobStoreType::S3 | io::BlobStoreType::GCS | io::BlobStoreType::Azure => {
                io::object_store::partitions_from_uri(&$table_source, uri, $call_with_r, &$ctx)
                    .await
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

pub use datafusion::{self, arrow, sql::sqlparser};

#[cfg(test)]
pub mod test_util;
