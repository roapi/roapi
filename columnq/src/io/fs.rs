use std::fs;

use datafusion::physical_plan::common::build_file_list;
use uriparse::URIReference;

use crate::error::ColumnQError;
use crate::table::TableSource;

pub fn partitions_from_uri<'a, F, T>(
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
