use snafu::prelude::*;
use uriparse::URIReference;

use crate::io;
use crate::table::{self, TableSource};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("GET error for {uri}: {source}"))]
    Get { uri: String, source: reqwest::Error },
    #[snafu(display("Invalid GET status {status} for {uri}: {resp}"))]
    Status {
        status: reqwest::StatusCode,
        uri: String,
        resp: String,
    },
    #[snafu(display("Failed to read bytes for {uri}: {source}"))]
    ReadBytes { uri: String, source: reqwest::Error },
    #[snafu(display("Could not load table data"))]
    Table { source: table::Error },
    #[snafu(display("Invalid header: {source}"))]
    HeaderName {
        source: reqwest::header::InvalidHeaderName,
    },
    #[snafu(display("Invalid header value: {source}"))]
    HeaderValue {
        source: reqwest::header::InvalidHeaderValue,
    },
    #[snafu(display("Failed to build http client: {source}"))]
    BuildClient { source: reqwest::Error },
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        Self::Generic {
            backend: "http",
            source: Box::new(err),
        }
    }
}

pub async fn partitions_from_uri<'a, F, T>(
    t: &'a TableSource,
    _uri: URIReference<'a>,
    mut partition_reader: F,
) -> Result<Vec<T>, io::Error>
where
    F: FnMut(std::io::Cursor<bytes::Bytes>) -> Result<T, table::Error>,
{
    let uri = t.get_uri_str();
    let mut req = reqwest::Client::builder()
        .build()
        .context(BuildClientSnafu)?
        .get(uri);

    // if config specified header override, apply it
    if let Some(io::IoOption::http {
        headers: Some(headers),
    }) = &t.io_option
    {
        let mut hdr_map = reqwest::header::HeaderMap::new();
        for (k, v) in headers.iter() {
            hdr_map.insert(
                reqwest::header::HeaderName::from_bytes(k.as_bytes()).context(HeaderNameSnafu)?,
                reqwest::header::HeaderValue::from_str(v).context(HeaderValueSnafu)?,
            );
        }
        req = req.headers(hdr_map);
    }

    let resp = req.send().await.context(GetSnafu { uri })?;

    if resp.status().as_u16() / 100 != 2 {
        Err(Error::Status {
            status: resp.status(),
            uri: uri.to_string(),
            resp: format!("{resp:?}"),
        })?;
    }
    let reader = std::io::Cursor::new(resp.bytes().await.context(ReadBytesSnafu { uri })?);

    // HTTP store doesn't support directory listing, so we always only return a single partition
    Ok(vec![partition_reader(reader).context(TableSnafu)?])
}
