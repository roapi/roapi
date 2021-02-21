use std::fs;

use datafusion::physical_plan::common::build_file_list;
use uriparse::URIReference;

use crate::error::ColumnQError;
use crate::table::TableSource;

pub fn partitions_from_fs_uri<'a, F, T>(
    t: &'a TableSource,
    uri: URIReference<'a>,
    mut partition_reader: F,
) -> Result<Vec<T>, ColumnQError>
where
    F: FnMut(std::fs::File) -> Result<T, ColumnQError>,
{
    let fs_path = uri.path().to_string();
    let mut files = vec![];
    let mut file_ext = ".".to_string();
    file_ext.push_str(t.extension()?);
    build_file_list(&fs_path, &mut files, &file_ext)?;

    // TODO: load partitions in parallel
    let partitions = files
        .iter()
        .map(|fpath| {
            let reader = fs::File::open(fpath)
                .map_err(|e| ColumnQError::FileStore(format!("open file error: {}", e)))?;

            partition_reader(reader)
        })
        .collect::<Result<Vec<T>, ColumnQError>>()?;

    Ok(partitions)
}

pub async fn partitions_from_http_uri<'a, F, T>(
    t: &'a TableSource,
    _uri: URIReference<'a>,
    mut partition_reader: F,
) -> Result<Vec<T>, ColumnQError>
where
    F: FnMut(std::io::Cursor<bytes::Bytes>) -> Result<T, ColumnQError>,
{
    let resp = reqwest::get(&t.uri)
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

    // HTTP store doesn't support directory listing, so we always only return a single partition
    Ok(vec![partition_reader(reader)?])
}
