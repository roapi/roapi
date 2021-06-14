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

    let partitions: Vec<Vec<RecordBatch>> =
        partitions_from_table_source!(t, |mut r| -> Result<Vec<RecordBatch>, ColumnQError> {
            // TODO: this is very inefficient, we are copying the parquet data in memory twice when
            // it's being fetched from http store
            let mut buffer = Vec::new();
            r.read_to_end(&mut buffer).map_err(|_| {
                ColumnQError::LoadParquet("failed to copy parquet data into memory".to_string())
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
                Some(s) if s != &batch_schema => Schema::try_merge(vec![s.clone(), batch_schema])?,
                _ => batch_schema,
            });

            Ok(record_batch_reader
                .into_iter()
                .collect::<arrow::error::Result<Vec<RecordBatch>>>()?)
        })?;

    if partitions.is_empty() {
        return Err(ColumnQError::LoadParquet(
            "no parquet file found".to_string(),
        ));
    }

    Ok(datafusion::datasource::MemTable::try_new(
        Arc::new(
            schema.ok_or_else(|| {
                ColumnQError::LoadParquet("schema not found for table".to_string())
            })?,
        ),
        partitions,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use datafusion::datasource::TableProvider;

    use crate::table::TableLoadOption;
    use crate::test_util::*;

    #[tokio::test]
    async fn load_simple_parquet() -> Result<(), ColumnQError> {
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

    #[tokio::test]
    async fn load_partitions() -> anyhow::Result<()> {
        let tmp_dir = tempdir::TempDir::new("columnq.test.parquet_partitions")?;
        let tmp_dir_path = tmp_dir.path();

        let source_path = test_data_path("blogs.parquet");
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-01.parquet"))? > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-02.parquet"))? > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-03.parquet"))? > 0);

        let t = to_mem_table(&TableSource {
            name: "blogs".to_string(),
            uri: tmp_dir_path.to_string_lossy().to_string(),
            schema: None,
            option: Some(TableLoadOption::parquet {}),
        })
        .await?;

        assert_eq!(
            t.schema()
                .metadata()
                .get("writer.model.name")
                .map(|s| s.as_str()),
            Some("protobuf")
        );

        assert_eq!(t.statistics().num_rows, Some(1500));

        Ok(())
    }
}
