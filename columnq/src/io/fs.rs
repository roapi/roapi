use std::fs;

use datafusion::physical_plan::common::build_file_list;
use log::debug;
use uriparse::URIReference;

use crate::error::ColumnQError;
use crate::table::TableSource;

pub fn partitions_from_iterator<'a, F, T, I>(
    path_iter: I,
    mut partition_reader: F,
) -> Result<Vec<T>, ColumnQError>
where
    I: Iterator<Item = &'a str>,
    F: FnMut(std::fs::File) -> Result<T, ColumnQError>,
{
    // TODO: load partitions in parallel
    let partitions = path_iter
        .map(|fpath| {
            debug!("loading file from path: {}", fpath);
            let reader = fs::File::open(fpath)
                .map_err(|e| ColumnQError::FileStore(format!("open file error: {e}")))?;

            partition_reader(reader)
        })
        .collect::<Result<Vec<T>, ColumnQError>>()?;

    Ok(partitions)
}

pub fn partitions_from_uri<'a, F, T>(
    t: &'a TableSource,
    uri: URIReference<'a>,
    partition_reader: F,
) -> Result<Vec<T>, ColumnQError>
where
    F: FnMut(std::fs::File) -> Result<T, ColumnQError>,
{
    let fs_path = uri.path().to_string();
    let mut file_ext = ".".to_string();
    file_ext.push_str(t.extension()?);
    debug!("building file list from path {}...", fs_path);
    let files = build_file_list(&fs_path, &file_ext).map_err(|e| {
        ColumnQError::FileStore(format!(
            "Failed to build file list from path `{fs_path}`: {e}"
        ))
    })?;

    debug!("loading file partitions: {:?}", files);
    partitions_from_iterator(files.iter().map(|s| s.as_str()), partition_reader)
}
