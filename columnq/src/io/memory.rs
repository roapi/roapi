use snafu::prelude::*;

use crate::io;
use crate::table::{self, TableSource};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("GET error for {uri}: {source}"))]
    Get { uri: String, source: reqwest::Error },

    #[snafu(display("Could not load table data"))]
    Table { source: table::Error },
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        Self::Generic {
            backend: "memory",
            source: Box::new(err),
        }
    }
}

pub async fn partitions_from_memory<'a, F, T>(
    t: &'a TableSource,
    mut partition_reader: F,
) -> Result<Vec<T>, io::Error>
where
    F: FnMut(std::io::Cursor<&'a [u8]>) -> Result<T, table::Error>,
{
    let data = t.io_source.as_memory().context(TableSnafu)?;
    let reader = std::io::Cursor::new(data);
    // There is no concept of directory listing for in memory data, so we always only return a
    // single partition
    Ok(vec![partition_reader(reader).context(TableSnafu)?])
}
