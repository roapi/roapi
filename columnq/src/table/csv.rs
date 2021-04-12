use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use log::debug;

use crate::error::ColumnQError;
use crate::table::TableSource;

pub async fn to_mem_table(
    t: &TableSource,
) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    // TODO: read csv option from config
    let has_header = true;
    let delimiter = b',';
    let batch_size = 1024;
    let projection = None;

    debug!("inferring csv table schema...");
    let schema_ref: arrow::datatypes::SchemaRef = match &t.schema {
        Some(s) => Arc::new(s.into()),
        None => {
            let schemas = partitions_from_table_source!(t, |mut r| {
                let (schema, record_count) =
                    arrow::csv::reader::infer_reader_schema(&mut r, delimiter, None, has_header)?;

                if record_count > 0 {
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

    debug!("loading csv table data...");
    let partitions: Vec<Vec<RecordBatch>> =
        partitions_from_table_source!(t, |r| -> Result<Vec<RecordBatch>, ColumnQError> {
            let csv_reader = arrow::csv::Reader::new(
                r,
                schema_ref.clone(),
                has_header,
                Some(delimiter),
                batch_size,
                None,
                projection.clone(),
            );

            csv_reader
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
        let tmp_dir = tempdir::TempDir::new("columnq.test.csv_partitions")?;
        let tmp_dir_path = tmp_dir.path();

        let source_path = test_data_path("uk_cities_with_headers.csv");
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-01.csv"))? > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-02.csv"))? > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-03.csv"))? > 0);

        let t = to_mem_table(&TableSource {
            name: "uk_cities".to_string(),
            uri: tmp_dir_path.to_string_lossy().to_string(),
            schema: None,
            option: Some(TableLoadOption::csv {}),
        })
        .await?;

        assert_eq!(t.statistics().num_rows, Some(37 * 3));

        Ok(())
    }
}
