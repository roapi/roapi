use std::fs;

use datafusion::physical_plan::common::build_file_list;
use log::debug;
use snafu::prelude::*;
use uriparse::URIReference;

use crate::io;
use crate::table::{self, TableIoSource, TableSource};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Could not load table data: {source}"))]
    Table { source: table::Error },
    #[snafu(display("Could not resolve table extension {table_io_source}: {source}"))]
    TableExtension {
        table_io_source: TableIoSource,
        source: table::Error,
    },
    #[snafu(display(
        "Failed to build file list for path `{fs_path}` with ext `{file_ext}`: {source}"
    ))]
    FileList {
        fs_path: String,
        file_ext: String,
        source: datafusion::error::DataFusionError,
    },
    #[snafu(display("Failed to open file `{fpath}`: {source}"))]
    FileOpen {
        fpath: String,
        source: std::io::Error,
    },
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        Self::Generic {
            backend: "fs",
            source: Box::new(err),
        }
    }
}

pub fn partitions_from_iterator<'a, F, T, I>(
    path_iter: I,
    mut partition_reader: F,
) -> Result<Vec<T>, io::Error>
where
    I: Iterator<Item = &'a str>,
    F: FnMut(std::fs::File) -> Result<T, table::Error>,
{
    // TODO: load partitions in parallel
    let partitions = path_iter
        .map(|fpath| {
            debug!("loading file from path: {fpath}");
            let reader = fs::File::open(fpath).context(FileOpenSnafu { fpath })?;

            partition_reader(reader).context(TableSnafu)
        })
        .collect::<Result<Vec<T>, Error>>()?;

    Ok(partitions)
}

pub fn partitions_from_uri<'a, F, T>(
    t: &'a TableSource,
    uri: URIReference<'a>,
    partition_reader: F,
) -> Result<Vec<T>, io::Error>
where
    F: FnMut(std::fs::File) -> Result<T, table::Error>,
{
    let fs_path = uri.path().to_string();
    let mut file_ext = ".".to_string();
    file_ext.push_str(
        t.extension()
            .context(TableExtensionSnafu {
                table_io_source: t.io_source.clone(),
            })?
            .into(),
    );
    debug!("building file list from path {fs_path}...");
    let files =
        build_file_list(&fs_path, &file_ext).context(FileListSnafu { fs_path, file_ext })?;

    debug!("loading file partitions: {files:?}");
    partitions_from_iterator(files.iter().map(|s| s.as_str()), partition_reader)
}
