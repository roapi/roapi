use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use log::debug;

use crate::error::ColumnQError;
use crate::table::TableSource;

pub async fn to_mem_table(
    t: &TableSource,
) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    debug!("inferring arrow table schema...");
    let schema_ref: arrow::datatypes::SchemaRef = match &t.schema {
        Some(s) => Arc::new(s.into()),
        None => {
            let schemas = partitions_from_table_source!(t, |mut r| {
                let arrow_stream_reader = arrow::ipc::reader::StreamReader::try_new(&mut r)?;
                let schema = (*arrow_stream_reader.schema()).clone();
                if arrow_stream_reader.into_iter().next().is_some() {
                    Ok(Some(schema))
                } else {
                    Ok(None)
                }
            })?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

            Arc::new(Schema::try_merge(schemas)?)
        }
    };

    debug!("loading arrow table data...");
    let partitions: Vec<Vec<RecordBatch>> =
        partitions_from_table_source!(t, |mut r| -> Result<Vec<RecordBatch>, ColumnQError> {
            let arrow_stream_reader = arrow::ipc::reader::StreamReader::try_new(&mut r)?;
            arrow_stream_reader
                .into_iter()
                .map(|batch| Ok(batch?))
                .collect::<Result<Vec<RecordBatch>, ColumnQError>>()
        })?;

    Ok(datafusion::datasource::MemTable::try_new(
        schema_ref, partitions,
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
    async fn load_partitions() -> anyhow::Result<()> {
        let tmp_dir = tempdir::TempDir::new("columnq.test.arrows_partitions")?;
        let tmp_dir_path = tmp_dir.path();

        let source_path = test_data_path("uk_cities_with_headers.arrows");
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-01.arrows"))? > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-02.arrows"))? > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-03.arrows"))? > 0);

        let t = to_mem_table(
            &TableSource::new(
                "uk_cities".to_string(),
                tmp_dir_path.to_string_lossy().to_string(),
            )
            .with_option(TableLoadOption::arrows {}),
        )
        .await?;

        assert_eq!(t.statistics().num_rows, Some(37 * 3));

        Ok(())
    }

    #[tokio::test]
    async fn load_file() -> anyhow::Result<()> {
        let test_path = test_data_path("uk_cities_with_headers.arrows");

        let t = to_mem_table(&TableSource::new("uk_cities".to_string(), test_path)).await?;

        assert_eq!(t.statistics().num_rows, Some(37));

        Ok(())
    }
}
