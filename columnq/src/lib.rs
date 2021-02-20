#[macro_use]
extern crate lazy_static;

pub mod error;

macro_rules! with_reader_from_uri {
    ($call_with_r:expr, $uri:ident) => {
        match $uri.scheme() {
            // default to local file when schema is not provided
            None | Some(uriparse::Scheme::FileSystem) => {
                crate::io::partitions_from_fs_uri(&$uri, $call_with_r)
            }
            Some(uriparse::Scheme::HTTP) | Some(uriparse::Scheme::HTTPS) => {
                crate::io::partitions_from_http_uri(&$uri, $call_with_r).await
            }
            // "s3" => {}
            _ => Err(ColumnQError::InvalidUri(format!(
                "Unsupported scheme in table uri: {:?}",
                $uri
            ))),
        }
    };
}

pub mod columnq;
pub mod io;
pub mod query;
pub mod table;

pub use crate::columnq::*;

#[cfg(test)]
pub mod test_util;
