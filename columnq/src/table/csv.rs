use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use log::debug;

use crate::error::ColumnQError;
use crate::table::{TableLoadOption, TableOptionCsv, TableSource};

pub async fn to_mem_table(
    t: &TableSource,
) -> Result<datafusion::datasource::MemTable, ColumnQError> {
    let opt = t
        .option
        .clone()
        .unwrap_or_else(|| TableLoadOption::csv(TableOptionCsv::default()));
    let opt = opt.as_csv()?;

    let has_header = opt.has_header;
    let delimiter = opt.delimiter;
    let projection = opt.projection.as_ref();

    let batch_size = t.batch_size;

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
                projection.cloned(),
                None,
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

    use datafusion::datasource::TableProvider;
    use std::fs;

    use crate::table::{TableIoSource, TableLoadOption};
    use crate::test_util::*;

    #[tokio::test]
    async fn load_partitions() -> anyhow::Result<()> {
        let tmp_dir = tempdir::TempDir::new("columnq.test.csv_partitions")?;
        let tmp_dir_path = tmp_dir.path();

        let source_path = test_data_path("uk_cities_with_headers.csv");
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-01.csv"))? > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-02.csv"))? > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-03.csv"))? > 0);

        let t = to_mem_table(
            &TableSource::new(
                "uk_cities".to_string(),
                tmp_dir_path.to_string_lossy().to_string(),
            )
            .with_option(TableLoadOption::csv(
                TableOptionCsv::default().with_has_header(true),
            )),
        )
        .await?;

        let stats = t.scan(&None, &[], None).await?.statistics();
        assert_eq!(stats.num_rows, Some(37 * 3));

        Ok(())
    }

    #[tokio::test]
    async fn load_from_memory() -> anyhow::Result<()> {
        let csv_content = r#"
c1,c2,c3
1,"hello",true
2,"world",true
100,"!",false
"#
        .to_string();

        let source = TableSource::new("test", TableIoSource::Memory(csv_content.into_bytes()))
            .with_option(TableLoadOption::csv(
                TableOptionCsv::default().with_has_header(true),
            ));
        let t = to_mem_table(&source).await?;

        let stats = t.scan(&None, &[], None).await?.statistics();
        assert_eq!(stats.num_rows, Some(3));

        Ok(())
    }
}
