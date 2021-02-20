use std::fs;

use uriparse::URIReference;

use crate::error::ColumnQError;

pub fn partitions_from_fs_uri<F, T>(
    uri: &URIReference,
    mut partition_reader: F,
) -> Result<T, ColumnQError>
where
    F: FnMut(std::fs::File) -> Result<T, ColumnQError>,
{
    let reader = fs::File::open(uri.path().to_string())
        .map_err(|e| ColumnQError::FileStore(format!("open file error: {}", e)))?;
    partition_reader(reader)
}

pub async fn partitions_from_http_uri<'a, 'b, F, T>(
    uri: &'a URIReference<'b>,
    mut partition_reader: F,
) -> Result<T, ColumnQError>
where
    F: FnMut(std::io::Cursor<bytes::Bytes>) -> Result<T, ColumnQError>,
{
    let resp = reqwest::get(&uri.to_string())
        .await
        .map_err(|e| ColumnQError::HttpStore(e.to_string()))?;
    if resp.status().as_u16() / 100 != 2 {
        return Err(ColumnQError::HttpStore(format!(
            "Invalid response from server: {:?}",
            resp
        )));
    }
    let reader = std::io::Cursor::new(resp.bytes().await.map_err(|e| {
        ColumnQError::HttpStore(format!("Failed to decode server response: {}", e))
    })?);
    partition_reader(reader)
}
