use serde_derive::Deserialize;
use std::convert::TryFrom;

#[derive(Deserialize, Default, Clone, Copy)]
pub enum ContentType {
    #[default]
    Json,
    Csv,
    ArrowFile,
    ArrowStream,
    Parquet,
}

impl ContentType {
    pub fn to_str(&self) -> &'static str {
        match self {
            ContentType::Json => "application/json",
            ContentType::Csv => "application/csv",
            ContentType::ArrowFile => "application/vnd.apache.arrow.file",
            ContentType::ArrowStream => "application/vnd.apache.arrow.stream",
            ContentType::Parquet => "application/parquet",
        }
    }
}

impl TryFrom<&[u8]> for ContentType {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, ()> {
        match value {
            b"*/*" | b"application/json" => Ok(ContentType::Json),
            b"application/csv" => Ok(ContentType::Csv),
            b"application/arrow.file" | b"application/vnd.apache.arrow.file" => {
                Ok(ContentType::ArrowFile)
            }
            b"application/arrow.stream" | b"application/vnd.apache.arrow.stream" => {
                Ok(ContentType::ArrowStream)
            }
            b"application/parquet" | b"application/vnd.apache.parquet" => Ok(ContentType::Parquet),
            _ => Err(()),
        }
    }
}

pub mod arrow;
pub mod csv;
pub mod json;
pub mod parquet;
