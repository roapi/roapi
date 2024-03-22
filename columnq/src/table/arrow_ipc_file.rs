use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use log::debug;
use snafu::prelude::*;

use crate::table::{self, TableSource};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Faild to create IPC file reader: {source}"))]
    NewReader {
        source: datafusion::arrow::error::ArrowError,
    },

    #[snafu(display("Failed to collect record batches: {source}"))]
    CollectRecordBatch {
        source: datafusion::arrow::error::ArrowError,
    },
}

pub async fn to_mem_table(
    t: &TableSource,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<datafusion::datasource::MemTable, table::Error> {
    debug!("loading arrow table data...");
    let mut schema_and_partitions = partitions_from_table_source!(
        t,
        |mut r| {
            let arrow_file_reader = arrow::ipc::reader::FileReader::try_new(&mut r, None)
                .context(NewReaderSnafu)
                .context(table::LoadArrowIpcFileSnafu)?;
            let schema = (*arrow_file_reader.schema()).clone();

            arrow_file_reader
                .into_iter()
                .collect::<Result<Vec<RecordBatch>, _>>()
                .map(|batches| (Some(schema), batches))
                .context(CollectRecordBatchSnafu)
                .context(table::LoadArrowIpcFileSnafu)
        },
        dfctx
    )
    .context(table::IoSnafu)?;

    let schema_ref = match &t.schema {
        Some(s) => Arc::new(s.into()),
        None => {
            debug!("inferring arrow stream schema...");
            Arc::new(
                Schema::try_merge(
                    schema_and_partitions
                        .iter_mut()
                        .flat_map(|v| if !(v.1).is_empty() { v.0.take() } else { None })
                        .collect::<Vec<_>>(),
                )
                .context(table::MergeSchemaSnafu)?,
            )
        }
    };

    datafusion::datasource::MemTable::try_new(
        schema_ref,
        schema_and_partitions
            .into_iter()
            .map(|v| v.1)
            .collect::<Vec<Vec<RecordBatch>>>(),
    )
    .context(table::CreateMemTableSnafu)
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::common::stats::Precision;
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;
    use std::fs;
    use tempfile::Builder;

    use crate::table::TableLoadOption;
    use crate::test_util::*;

    #[tokio::test]
    async fn load_partitions() {
        let ctx = SessionContext::new();
        let tmp_dir = Builder::new()
            .prefix("columnq.test.arrows_partitions")
            .tempdir()
            .unwrap();
        let tmp_dir_path = tmp_dir.path();

        let source_path = test_data_path("uk_cities_with_headers.arrow");
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-01.arrow")).unwrap() > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-02.arrow")).unwrap() > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-03.arrow")).unwrap() > 0);

        let t = to_mem_table(
            &TableSource::new(
                "uk_cities".to_string(),
                tmp_dir_path.to_string_lossy().to_string(),
            )
            .with_option(TableLoadOption::arrow {}),
            &ctx,
        )
        .await
        .unwrap();

        let stats = t
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap()
            .statistics()
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(37 * 3));
    }

    #[tokio::test]
    async fn load_file() {
        let ctx = SessionContext::new();
        let test_path = test_data_path("uk_cities_with_headers.arrow");

        let t = to_mem_table(&TableSource::new("uk_cities".to_string(), test_path), &ctx)
            .await
            .unwrap();

        let stats = t
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap()
            .statistics()
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(37));
    }
}
