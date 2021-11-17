use std::io::Read;
use std::sync::Arc;

use crate::error::ColumnQError;
use crate::table::{TableLoadOption, TableOptionParquet, TableSource};

use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::parquet::ParquetTable;
use datafusion::datasource::TableProvider;
use datafusion::parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use datafusion::parquet::file::reader::SerializedFileReader;
use datafusion::parquet::file::serialized_reader::SliceableCursor;

pub async fn to_datafusion_table(t: &TableSource) -> Result<Arc<dyn TableProvider>, ColumnQError> {
    let opt = t
        .option
        .clone()
        .unwrap_or_else(|| TableLoadOption::parquet(TableOptionParquet::default()));
    let TableOptionParquet { use_memory_table } = opt.as_parquet()?;

    if *use_memory_table {
        to_mem_table(t).await
    } else {
        Ok(Arc::new(
            ParquetTable::try_new(t.parsed_uri()?, 4).map_err(|err| {
                ColumnQError::LoadParquet(format!("failed to load parquet: '{}'", err.to_string()))
            })?,
        ))
    }
}

pub async fn to_mem_table(t: &TableSource) -> Result<Arc<dyn TableProvider>, ColumnQError> {
    let batch_size = t.batch_size;

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

    let table =
        Arc::new(datafusion::datasource::MemTable::try_new(
            Arc::new(schema.ok_or_else(|| {
                ColumnQError::LoadParquet("schema not found for table".to_string())
            })?),
            partitions,
        )?);

    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use crate::table::TableLoadOption;
    use crate::test_util::*;

    #[tokio::test]
    async fn load_flattened_parquet() -> Result<(), ColumnQError> {
        let t = to_datafusion_table(
            &TableSource::new(
                "blogs".to_string(),
                test_data_path("blogs_flattened.parquet"),
            )
            .with_option(TableLoadOption::parquet(TableOptionParquet {
                use_memory_table: false,
            })),
        )
        .await?;

        let stats = t.scan(&None, 1024, &[], None)?.statistics();
        assert_eq!(stats.num_rows, Some(500));
        let stats = stats.column_statistics.unwrap();
        assert_eq!(stats[0].null_count, Some(245));
        assert_eq!(stats[1].null_count, Some(373));
        assert_eq!(stats[2].null_count, Some(237));

        match t.as_any().downcast_ref::<ParquetTable>() {
            Some(_) => Ok(()),
            None => panic!("must be of type datafusion::datasource::parquet::ParquetTable"),
        }
    }

    #[tokio::test]
    async fn load_simple_parquet() -> Result<(), ColumnQError> {
        let t = to_mem_table(&TableSource::new(
            "blogs".to_string(),
            test_data_path("blogs.parquet"),
        ))
        .await?;

        let schema = t.schema();
        assert_eq!(
            schema
                .metadata()
                .get("writer.model.name")
                .map(|s| s.as_str()),
            Some("protobuf")
        );

        let stats = t.scan(&None, 1024, &[], None)?.statistics();
        assert_eq!(stats.num_rows, Some(500));

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

        let t = to_mem_table(
            &TableSource::new_with_uri("blogs", tmp_dir_path.to_string_lossy())
                .with_option(TableLoadOption::parquet(TableOptionParquet::default())),
        )
        .await?;

        assert_eq!(
            t.schema()
                .metadata()
                .get("writer.model.name")
                .map(|s| s.as_str()),
            Some("protobuf")
        );

        let stats = t.scan(&None, 1024, &[], None)?.statistics();
        assert_eq!(stats.num_rows, Some(1500));

        Ok(())
    }
}
