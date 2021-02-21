#![deny(warnings)]

#[macro_use]
extern crate lazy_static;

pub mod error;

macro_rules! partitions_from_table_source {
    ($table_source:ident, $call_with_r:expr) => {{
        let uri = $table_source.parsed_uri()?;

        match uri.scheme() {
            // default to local file when schema is not provided
            None | Some(uriparse::Scheme::FileSystem) => {
                crate::io::partitions_from_fs_uri(&$table_source, uri, $call_with_r)
            }
            Some(uriparse::Scheme::HTTP) | Some(uriparse::Scheme::HTTPS) => {
                crate::io::partitions_from_http_uri(&$table_source, uri, $call_with_r).await
            }
            // "s3" => {}
            _ => Err(ColumnQError::InvalidUri(format!(
                "Unsupported scheme in table uri: {:?}",
                $table_source.uri,
            ))),
        }
    }};
}

pub mod columnq;
pub mod io;
pub mod query;
pub mod table;

pub use crate::columnq::*;

#[cfg(test)]
pub mod test_util;
