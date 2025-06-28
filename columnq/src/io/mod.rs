use std::collections::HashMap;

use serde_derive::Deserialize;
use snafu::prelude::*;

pub mod fs;
pub mod http;
pub mod memory;
pub mod object_store;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Generic {} error: {}", backend, source))]
    Generic {
        backend: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    #[snafu(display("Unsupported URI scheme: {scheme}"))]
    InvalidUriScheme { scheme: String },
}

#[allow(non_camel_case_types)]
#[derive(Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(tag = "protcol")]
#[serde(deny_unknown_fields)]
pub enum IoOption {
    http {
        headers: Option<HashMap<String, String>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobStoreType {
    Http,
    S3,
    GCS,
    Azure,
    FileSystem,
    Memory,
}

impl TryFrom<Option<&uriparse::Scheme<'_>>> for BlobStoreType {
    type Error = Error;

    fn try_from(scheme: Option<&uriparse::Scheme<'_>>) -> Result<Self, Self::Error> {
        match scheme {
            // default to local file when schema is not provided
            None | Some(uriparse::Scheme::FileSystem) | Some(uriparse::Scheme::File) => {
                Ok(BlobStoreType::FileSystem)
            }
            Some(uriparse::Scheme::HTTP) | Some(uriparse::Scheme::HTTPS) => Ok(BlobStoreType::Http),
            Some(uriparse::Scheme::Unregistered(s)) => BlobStoreType::try_from(s.as_str()),
            _ => Err(Error::InvalidUriScheme {
                scheme: format!("{scheme:?}"),
            }),
        }
    }
}

impl TryFrom<&str> for BlobStoreType {
    type Error = Error;

    fn try_from(scheme: &str) -> Result<Self, Self::Error> {
        match scheme {
            "http" | "https" => Ok(BlobStoreType::Http),
            "s3" => Ok(BlobStoreType::S3),
            "gs" => Ok(BlobStoreType::GCS),
            "az" | "adl" | "adfs" | "adfss" | "azure" => Ok(BlobStoreType::Azure),
            "memory" => Ok(BlobStoreType::Memory),
            _ => Err(Error::InvalidUriScheme {
                scheme: scheme.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::io::BlobStoreType;
    use uriparse::*;

    #[test]
    fn path_test() {
        // windows path
        let uri_ref = URIReference::try_from("file://d:/path/to/file.csv").unwrap();
        let blob_type = BlobStoreType::try_from(uri_ref.scheme()).unwrap();
        assert_eq!(blob_type, BlobStoreType::FileSystem);

        // old scheme & windows path
        let uri_ref = URIReference::try_from("filesystem://d:/path/to/file.csv").unwrap();
        let blob_type = BlobStoreType::try_from(uri_ref.scheme()).unwrap();
        assert_eq!(blob_type, BlobStoreType::FileSystem);

        // *nix path
        let uri_ref = URIReference::try_from("file://tmp/path/to/file.csv").unwrap();
        let blob_type = BlobStoreType::try_from(uri_ref.scheme()).unwrap();
        assert_eq!(blob_type, BlobStoreType::FileSystem);

        // *nix path, old scheme
        let uri_ref = URIReference::try_from("filesystem://tmp/path/to/file.csv").unwrap();
        let blob_type = BlobStoreType::try_from(uri_ref.scheme()).unwrap();
        assert_eq!(blob_type, BlobStoreType::FileSystem);

        // *nix path, no scheme
        let uri_ref = URIReference::try_from("/tmp/path/to/file.csv").unwrap();
        let blob_type = BlobStoreType::try_from(uri_ref.scheme()).unwrap();
        assert_eq!(blob_type, BlobStoreType::FileSystem);

        // *http
        let uri_ref = URIReference::try_from("http://tmp/path/to/file.csv").unwrap();
        let blob_type = BlobStoreType::try_from(uri_ref.scheme()).unwrap();
        assert_eq!(blob_type, BlobStoreType::Http);

        // *https
        let uri_ref = URIReference::try_from("https://tmp/path/to/file.csv").unwrap();
        let blob_type = BlobStoreType::try_from(uri_ref.scheme()).unwrap();
        assert_eq!(blob_type, BlobStoreType::Http);
    }
}
