#![feature(iterator_fold_self)]

#[macro_use]
extern crate lazy_static;

macro_rules! with_reader_from_uri {
    ($call_with_r:expr, $uri:ident) => {
        match $uri.scheme() {
            None => {
                let reader =
                    fs::File::open($uri.path().to_string()).context("Failed to read JSON data")?;
                $call_with_r(reader)
            }
            Some(Scheme::FileSystem) => {
                let reader =
                    fs::File::open($uri.path().to_string()).context("Failed to read JSON data")?;
                $call_with_r(reader)
            }
            Some(Scheme::HTTP) | Some(Scheme::HTTPS) => {
                let resp = reqwest::get(&$uri.to_string()).await?;
                if resp.status().as_u16() / 100 != 2 {
                    anyhow::bail!("Invalid response from server: {:?}", resp);
                }
                let reader =
                    Cursor::new(resp.bytes().await.context("Failed to read JSON data")?).reader();
                $call_with_r(reader)
            }
            // "s3" => {}
            _ => anyhow::bail!("Unsupported scheme in table uri: {:?}", $uri),
        }
    };
}

pub mod api;
pub mod config;
pub mod encoding;
pub mod error;
pub mod query;
pub mod table;
