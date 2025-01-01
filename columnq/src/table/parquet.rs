use std::io::Read;
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::record_batch::RecordBatchReader;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use snafu::prelude::*;

use crate::table::{
    self, datafusion_get_or_infer_schema, LoadedTable, TableLoadOption, TableOptionParquet,
    TableSource,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build parquet reader: {source}"))]
    BuildReader {
        source: datafusion::parquet::errors::ParquetError,
    },
    #[snafu(display("Failed to merge parquet schema: {source}"))]
    MergeSchema {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("Failed to collect record batches: {source}"))]
    CollectBatches {
        source: datafusion::arrow::error::ArrowError,
    },
    #[snafu(display("Failed to load parquety data into buffer: {source}"))]
    LoadBytes { source: std::io::Error },
    #[snafu(display("No parquet file found"))]
    EmptyPartition {},
    #[snafu(display("Schema not found"))]
    SchemaNotFound {},
    #[snafu(display("Failed to parse table uri: {source}"))]
    ParseUri {
        source: datafusion::error::DataFusionError,
    },
}

pub async fn to_loaded_table(
    t: TableSource,
    dfctx: datafusion::execution::context::SessionContext,
) -> Result<LoadedTable, table::Error> {
    LoadedTable::new_from_df_table_cb(move || to_datafusion_table(t.clone(), dfctx.clone())).await
}

async fn to_datafusion_table(
    t: TableSource,
    dfctx: datafusion::execution::context::SessionContext,
) -> Result<Arc<dyn TableProvider>, table::Error> {
    let opt = t
        .option
        .clone()
        .unwrap_or_else(|| TableLoadOption::parquet(TableOptionParquet::default()));
    let TableOptionParquet { use_memory_table } = opt.as_parquet()?;

    if *use_memory_table {
        to_mem_table(&t, &dfctx).await
    } else {
        let table_url = ListingTableUrl::parse(t.get_uri_str())
            .context(ParseUriSnafu)
            .context(table::LoadParquetSnafu)?;
        let mut options = ListingOptions::new(Arc::new(ParquetFormat::default()));
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
}

pub async fn to_mem_table(
    t: &TableSource,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<Arc<dyn TableProvider>, table::Error> {
    let batch_size = t.batch_size;

    let mut schema: Option<Schema> = None;

    let partitions: Vec<Vec<RecordBatch>> = partitions_from_table_source!(
        t,
        |mut r| -> Result<Vec<RecordBatch>, table::Error> {
            // TODO: this is very inefficient, we are copying the parquet data in memory twice when
            // it's being fetched from http store
            let mut buffer = Vec::new();
            r.read_to_end(&mut buffer)
                .context(LoadBytesSnafu)
                .context(table::LoadParquetSnafu)?;

            let record_batch_reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
                bytes::Bytes::from(buffer),
                ArrowReaderOptions::new(),
            )
            .context(BuildReaderSnafu)
            .context(table::LoadParquetSnafu)?
            .with_batch_size(batch_size)
            .build()
            .context(BuildReaderSnafu)
            .context(table::LoadParquetSnafu)?;

            let batch_schema = &*record_batch_reader.schema();
            schema = Some(match &schema {
                Some(s) if s != batch_schema => {
                    Schema::try_merge(vec![s.clone(), batch_schema.clone()])
                        .context(MergeSchemaSnafu)
                        .context(table::LoadParquetSnafu)?
                }
                _ => batch_schema.clone(),
            });

            record_batch_reader
                .into_iter()
                .collect::<arrow::error::Result<Vec<RecordBatch>>>()
                .context(CollectBatchesSnafu)
                .context(table::LoadParquetSnafu)
        },
        dfctx
    )
    .context(table::IoSnafu)?;

    if partitions.is_empty() {
        return Err(Error::EmptyPartition {}).context(table::LoadParquetSnafu);
    }

    let table = Arc::new(
        datafusion::datasource::MemTable::try_new(
            Arc::new(
                schema
                    .ok_or(Error::SchemaNotFound {})
                    .context(table::LoadParquetSnafu)?,
            ),
            partitions,
        )
        .context(table::CreateMemTableSnafu)?,
    );

    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::Builder;

    use crate::test_util::*;
    use datafusion::common::stats::Precision;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn load_flattened_parquet() {
        let ctx = SessionContext::new();
        let t = to_loaded_table(
            TableSource::new(
                "blogs".to_string(),
                test_data_path("blogs_flattened.parquet"),
            )
            .with_option(TableLoadOption::parquet(TableOptionParquet {
                use_memory_table: false,
            })),
            ctx.clone(),
        )
        .await
        .unwrap();

        let stats = t
            .table
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap()
            .statistics()
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(500));
        let stats = stats.column_statistics;
        assert_eq!(stats[0].null_count, Precision::Exact(245));
        assert_eq!(stats[1].null_count, Precision::Exact(373));
        assert_eq!(stats[2].null_count, Precision::Exact(237));

        match t.table.as_any().downcast_ref::<ListingTable>() {
            Some(_) => {}
            None => panic!("must be of type datafusion::datasource::listing::ListingTable"),
        }
    }

    #[tokio::test]
    async fn load_simple_parquet() {
        let ctx = SessionContext::new();
        let t = to_mem_table(
            &TableSource::new("blogs".to_string(), test_data_path("blogs.parquet")),
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
        assert_eq!(stats.num_rows, Precision::Exact(500));
    }

    #[tokio::test]
    async fn load_partitions() {
        let ctx = SessionContext::new();
        let tmp_dir = Builder::new()
            .prefix("columnq.test.parquet_partitions")
            .tempdir()
            .unwrap();
        let tmp_dir_path = tmp_dir.path();

        let source_path = test_data_path("blogs.parquet");
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-01.parquet")).unwrap() > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-02.parquet")).unwrap() > 0);
        assert!(fs::copy(&source_path, tmp_dir_path.join("2020-01-03.parquet")).unwrap() > 0);

        let t = to_mem_table(
            &TableSource::new_with_uri("blogs", tmp_dir_path.to_string_lossy())
                .with_option(TableLoadOption::parquet(TableOptionParquet::default())),
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
        assert_eq!(stats.num_rows, Precision::Exact(1500));
    }
}
