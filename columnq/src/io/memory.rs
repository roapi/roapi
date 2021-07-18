use crate::error::ColumnQError;
use crate::table::TableSource;

pub async fn partitions_from_memory<'a, F, T>(
    t: &'a TableSource,
    mut partition_reader: F,
) -> Result<Vec<T>, ColumnQError>
where
    F: FnMut(std::io::Cursor<&'a [u8]>) -> Result<T, ColumnQError>,
{
    let reader = std::io::Cursor::new(t.io_source.as_memory()?);
    // There is no concept of directory listing for in memory data, so we always only return a
    // single partition
    Ok(vec![partition_reader(reader)?])
}
