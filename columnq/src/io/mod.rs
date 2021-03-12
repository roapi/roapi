use std::convert::TryFrom;

pub mod fs;
pub mod http;
pub mod s3;

use crate::error::ColumnQError;

pub enum BlobStoreType {
    Http,
    S3,
    FileSystem,
}

impl TryFrom<Option<&uriparse::Scheme<'_>>> for BlobStoreType {
    type Error = ColumnQError;

    fn try_from(scheme: Option<&uriparse::Scheme<'_>>) -> Result<Self, Self::Error> {
        match scheme {
            // default to local file when schema is not provided
            None | Some(uriparse::Scheme::FileSystem) => Ok(BlobStoreType::FileSystem),
            Some(uriparse::Scheme::HTTP) | Some(uriparse::Scheme::HTTPS) => Ok(BlobStoreType::Http),
            Some(uriparse::Scheme::Unregistered(s)) => match s.as_str() {
                "s3" => Ok(BlobStoreType::S3),
                _ => Err(ColumnQError::InvalidUri(format!(
                    "Unsupported scheme: {:?}",
                    scheme
                ))),
            },

            _ => Err(ColumnQError::InvalidUri(format!(
                "Unsupported scheme: {:?}",
                scheme
            ))),
        }
    }
}
