#[macro_use]
extern crate lazy_static;

pub mod error;

macro_rules! with_reader_from_uri {
    ($call_with_r:expr, $uri:ident) => {
        match $uri.scheme() {
            // default to local file when schema is not provided
            None | Some(Scheme::FileSystem) => {
                let reader = fs::File::open($uri.path().to_string())
                    .map_err(|e| ColumnQError::FileStore(format!("open file error: {}", e)))?;
                $call_with_r(reader).map_err(ColumnQError::json_parse)
            }
            Some(Scheme::HTTP) | Some(Scheme::HTTPS) => {
                let resp = reqwest::get(&$uri.to_string())
                    .await
                    .map_err(|e| ColumnQError::HttpStore(e.to_string()))?;
                if resp.status().as_u16() / 100 != 2 {
                    return Err(ColumnQError::HttpStore(format!(
                        "Invalid response from server: {:?}",
                        resp
                    )));
                }
                let reader = Cursor::new(resp.bytes().await.map_err(|e| {
                    ColumnQError::HttpStore(format!("Failed to decode server response: {}", e))
                })?);
                $call_with_r(reader).map_err(ColumnQError::json_parse)
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
pub mod query;
pub mod table;

pub use crate::columnq::*;

#[cfg(test)]
pub mod test_util;
