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
    debug!("loading arrow table data...");
    let mut schema_and_partitions = partitions_from_table_source!(t, |mut r| {
        let arrow_stream_reader = arrow::ipc::reader::StreamReader::try_new(&mut r, None)?;
        let schema = (*arrow_stream_reader.schema()).clone();

        arrow_stream_reader
            .into_iter()
            .map(|batch| Ok(batch?))
            .collect::<Result<Vec<RecordBatch>, ColumnQError>>()
            .map(|batches| (Some(schema), batches))
    })?;

    let schema_ref = match &t.schema {
        Some(s) => Arc::new(s.into()),
        None => {
            debug!("inferring arrow stream schema...");
            Arc::new(Schema::try_merge(
                schema_and_partitions
                    .iter_mut()
                    .flat_map(|v| if !(v.1).is_empty() { v.0.take() } else { None })
                    .collect::<Vec<_>>(),
            )?)
        }
    };

    Ok(datafusion::datasource::MemTable::try_new(
        schema_ref,
        schema_and_partitions
            .into_iter()
            .map(|v| v.1)
            .collect::<Vec<Vec<RecordBatch>>>(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;
    use std::fs;
    use tempfile::Builder;

    use crate::table::TableLoadOption;
    use crate::test_util::*;

    #[tokio::test]
    async fn load_partitions() -> anyhow::Result<()> {
        let tmp_dir = Builder::new()
            .prefix("columnq.test.arrows_partitions")
            .tempdir()?;
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

        let ctx = SessionContext::new();
        let stats = t.scan(&ctx.state(), &None, &[], None).await?.statistics();
        assert_eq!(stats.num_rows, Some(37 * 3));

        Ok(())
    }

    #[tokio::test]
    async fn load_file() -> anyhow::Result<()> {
        let test_path = test_data_path("uk_cities_with_headers.arrows");

        let t = to_mem_table(&TableSource::new("uk_cities".to_string(), test_path)).await?;

        let ctx = SessionContext::new();
        let stats = t.scan(&ctx.state(), &None, &[], None).await?.statistics();
        assert_eq!(stats.num_rows, Some(37));

        Ok(())
    }
}
