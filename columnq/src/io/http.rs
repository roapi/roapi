use uriparse::URIReference;

use crate::error::ColumnQError;
use crate::table::TableSource;

pub async fn partitions_from_uri<'a, F, T>(
    t: &'a TableSource,
    _uri: URIReference<'a>,
    mut partition_reader: F,
) -> Result<Vec<T>, ColumnQError>
where
    F: FnMut(std::io::Cursor<bytes::Bytes>) -> Result<T, ColumnQError>,
{
    let resp = reqwest::get(t.get_uri_str())
        .await
        .map_err(|e| ColumnQError::HttpStore(e.to_string()))?;
    if resp.status().as_u16() / 100 != 2 {
        return Err(ColumnQError::HttpStore(format!(
            "Invalid response from server: {resp:?}"
        )));
    }
    let reader =
        std::io::Cursor::new(resp.bytes().await.map_err(|e| {
            ColumnQError::HttpStore(format!("Failed to decode server response: {e}"))
        })?);

    // HTTP store doesn't support directory listing, so we always only return a single partition
    Ok(vec![partition_reader(reader)?])
}
