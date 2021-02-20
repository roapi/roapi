use std::io::Read;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;
use parquet::file::serialized_reader::SliceableCursor;

use crate::error::ColumnQError;
use crate::table::TableSource;

pub async fn to_mem_table(
    t: &TableSource,
) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    // TODO: make batch size configurable
    let batch_size = 1024;

    let mut schema: Option<Schema> = None;

    let uri = t.parsed_uri()?;

    let partition: Vec<RecordBatch> = with_reader_from_uri!(
        |mut r| -> Result<Vec<RecordBatch>, ColumnQError> {
            // TODO: this is very inefficient, we are copying the parquet data in memory twice when
            // it's being fetched from http store
            let mut buffer = Vec::new();
            r.read_to_end(&mut buffer).map_err(|_| {
                ColumnQError::LoadParquet("failed to copy parquet data in memory".to_string())
            })?;

            let file_reader = SerializedFileReader::new(SliceableCursor::new(buffer))
                .map_err(ColumnQError::parquet_file_reader)?;
            let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

            let record_batch_reader = arrow_reader
                .get_record_reader(batch_size)
                .map_err(ColumnQError::parquet_record_reader)?;

            let batch_schema = arrow_reader.get_schema().map_err(|_| {
                ColumnQError::LoadParquet("failed to load schema from partition".to_string())
            })?;
            schema = Some(match &schema {
                Some(s) => Schema::try_merge(vec![s.clone(), batch_schema])?,
                None => batch_schema,
            });

            Ok(record_batch_reader
                .into_iter()
                .collect::<arrow::error::Result<Vec<RecordBatch>>>()?)
        },
        uri
    )?;

    Ok(datafusion::datasource::MemTable::try_new(
        Arc::new(
            schema.ok_or_else(|| ColumnQError::LoadParquet("failed to load schema".to_string()))?,
        ),
        vec![partition],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::datasource::TableProvider;

    use crate::test_util::*;

    #[tokio::test]
    async fn simple_parquet_load() -> Result<(), ColumnQError> {
        let t = to_mem_table(&TableSource {
            name: "blogs".to_string(),
            uri: test_data_path("blogs.parquet"),
            schema: None,
            option: None,
        })
        .await?;

        let schema = t.schema();
        assert_eq!(
            schema
                .metadata()
                .get("writer.model.name")
                .map(|s| s.as_str()),
            Some("protobuf")
        );

        assert_eq!(t.statistics().num_rows, Some(500));

        Ok(())
    }
}
