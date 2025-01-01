use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use log::debug;
use snafu::prelude::*;

use crate::table::{
    self, datafusion_get_or_infer_schema, LoadedTable, TableLoadOption, TableOptionCsv, TableSource,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read data into memory: {source}"))]
    ReadBytes {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("Failed to infer CSV schema: {source}"))]
    InferSchema {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("Failed to build CSV reader: {source}"))]
    BuildReader {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("Failed to merge CSV schema: {source}"))]
    MergeSchema {
        source: datafusion::arrow::error::ArrowError,
    },
}

async fn to_datafusion_table(
    t: TableSource,
    dfctx: datafusion::execution::context::SessionContext,
) -> Result<Arc<dyn TableProvider>, table::Error> {
    let opt = t
        .option
        .clone()
        .unwrap_or_else(|| TableLoadOption::csv(TableOptionCsv::default()));
    let opt = opt
        .as_csv()
        .expect("Invalid table format option, expect csv");
    if opt.use_memory_table {
        return to_mem_table(&t, &dfctx).await;
    }
    let table_url =
        ListingTableUrl::parse(t.get_uri_str()).with_context(|_| table::ListingTableUriSnafu {
            uri: t.get_uri_str().to_string(),
        })?;
    let mut options = ListingOptions::new(Arc::new(opt.as_df_csv_format()));
    if let Some(partition_cols) = t.datafusion_partition_cols() {
        options = options.with_table_partition_cols(partition_cols)
    }

    let schemaref = datafusion_get_or_infer_schema(
        &dfctx,
        &table_url,
        &options,
        &t.schema,
        &t.schema_from_files,
    )
    .await?;

    let table_config = ListingTableConfig::new(table_url)
        .with_listing_options(options)
        .with_schema(schemaref);
    Ok(Arc::new(
        ListingTable::try_new(table_config).context(table::CreateListingTableSnafu)?,
    ))
}

async fn to_mem_table(
    t: &TableSource,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<Arc<dyn TableProvider>, table::Error> {
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
            let fmt = opt.as_arrow_csv_format();
            let schemas = partitions_from_table_source!(
                t,
                |r| {
                    let (schema, record_count) = fmt
                        .infer_schema(r, None)
                        .context(InferSchemaSnafu)
                        .context(table::LoadCsvSnafu)?;
                    if record_count > 0 {
                        Ok(Some(schema))
                    } else {
                        Ok(None)
                    }
                },
                dfctx
            )
            .context(table::IoSnafu)?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

            Arc::new(
                Schema::try_merge(schemas)
                    .context(MergeSchemaSnafu)
                    .context(table::LoadCsvSnafu)?,
            )
        }
    };

    debug!("loading csv table data...");
    let partitions: Vec<Vec<RecordBatch>> = partitions_from_table_source!(
        t,
        |r| -> Result<Vec<RecordBatch>, table::Error> {
            let mut builder = arrow::csv::reader::ReaderBuilder::new(schema_ref.clone())
                .with_header(has_header)
                .with_delimiter(delimiter)
                .with_batch_size(batch_size);
            if let Some(p) = projection {
                builder = builder.with_projection(p.clone());
            }
            let csv_reader = builder
                .build(r)
                .context(BuildReaderSnafu)
                .context(table::LoadCsvSnafu)?;

            csv_reader
                .collect::<Result<Vec<RecordBatch>, _>>()
                .context(ReadBytesSnafu)
                .context(table::LoadCsvSnafu)
        },
        dfctx
    )
    .context(table::IoSnafu)?;

    let table = Arc::new(
        datafusion::datasource::MemTable::try_new(schema_ref, partitions)
            .context(table::CreateMemTableSnafu)?,
    );

    Ok(table)
}

pub async fn to_loaded_table(
    t: TableSource,
    dfctx: datafusion::execution::context::SessionContext,
) -> Result<LoadedTable, table::Error> {
    LoadedTable::new_from_df_table_cb(move || to_datafusion_table(t.clone(), dfctx.clone())).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::stats::Precision;
    use datafusion::prelude::SessionContext;
    use std::fs;
    use tempfile::Builder;

    use crate::table::TableIoSource;
    use crate::test_util::*;

    #[tokio::test]
    async fn load_partitions() {
        let ctx = SessionContext::new();
        let tmp_dir = Builder::new()
            .prefix("columnq.test.csv_partitions")
            .tempdir()
            .unwrap();
        let tmp_dir_path = tmp_dir.path();

        let source_path = test_data_path("uk_cities_with_headers.csv");
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-01.csv")).unwrap() > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-02.csv")).unwrap() > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-03.csv")).unwrap() > 0);

        let t = to_mem_table(
            &TableSource::new(
                "uk_cities".to_string(),
                tmp_dir_path.to_string_lossy().to_string(),
            )
            .with_option(TableLoadOption::csv(
                TableOptionCsv::default().with_has_header(true),
            )),
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
    async fn load_from_memory() {
        let ctx = SessionContext::new();
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
        let t = to_mem_table(&source, &ctx).await.unwrap();

        let stats = t
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap()
            .statistics()
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(3));
    }
}
