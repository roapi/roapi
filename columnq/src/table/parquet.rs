use std::io::Read;
use std::sync::Arc;

use crate::error::ColumnQError;
use crate::table::{TableLoadOption, TableOptionParquet, TableSource};

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

pub async fn to_datafusion_table(
    t: &TableSource,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<Arc<dyn TableProvider>, ColumnQError> {
    let opt = t
        .option
        .clone()
        .unwrap_or_else(|| TableLoadOption::parquet(TableOptionParquet::default()));
    let TableOptionParquet { use_memory_table } = opt.as_parquet()?;

    if *use_memory_table {
        to_mem_table(t).await
    } else {
        let table_url = ListingTableUrl::parse(t.get_uri_str())?;
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let schemaref = match &t.schema {
            Some(s) => Arc::new(s.into()),
            None => options.infer_schema(&dfctx.state(), &table_url).await?,
        };

        let table_config = ListingTableConfig::new(table_url)
            .with_listing_options(options)
            .with_schema(schemaref);
        Ok(Arc::new(ListingTable::try_new(table_config)?))
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

            let record_batch_reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
                bytes::Bytes::from(buffer),
                ArrowReaderOptions::new(),
            )?
            .with_batch_size(batch_size)
            .build()?;

            let batch_schema = &*record_batch_reader.schema();
            schema = Some(match &schema {
                Some(s) if s != batch_schema => {
                    Schema::try_merge(vec![s.clone(), batch_schema.clone()])?
                }
                _ => batch_schema.clone(),
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
    use tempfile::Builder;

    use crate::table::TableLoadOption;
    use crate::test_util::*;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn load_flattened_parquet() {
        let ctx = SessionContext::new();
        let t = to_datafusion_table(
            &TableSource::new(
                "blogs".to_string(),
                test_data_path("blogs_flattened.parquet"),
            )
            .with_option(TableLoadOption::parquet(TableOptionParquet {
                use_memory_table: false,
            })),
            &ctx,
        )
        .await
        .unwrap();

        let stats = t
            .scan(&ctx.state(), &None, &[], None)
            .await
            .unwrap()
            .statistics();
        assert_eq!(stats.num_rows, Some(500));
        let stats = stats.column_statistics.unwrap();
        assert_eq!(stats[0].null_count, Some(245));
        assert_eq!(stats[1].null_count, Some(373));
        assert_eq!(stats[2].null_count, Some(237));

        match t.as_any().downcast_ref::<ListingTable>() {
            Some(_) => {}
            None => panic!("must be of type datafusion::datasource::listing::ListingTable"),
        }
    }

    #[tokio::test]
    async fn load_simple_parquet() -> Result<(), ColumnQError> {
        let t = to_mem_table(&TableSource::new(
            "blogs".to_string(),
            test_data_path("blogs.parquet"),
        ))
        .await?;

        let ctx = SessionContext::new();
        let stats = t.scan(&ctx.state(), &None, &[], None).await?.statistics();
        assert_eq!(stats.num_rows, Some(500));

        Ok(())
    }

    #[tokio::test]
    async fn load_partitions() -> anyhow::Result<()> {
        let tmp_dir = Builder::new()
            .prefix("columnq.test.parquet_partitions")
            .tempdir()?;
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

        let ctx = SessionContext::new();
        let stats = t.scan(&ctx.state(), &None, &[], None).await?.statistics();
        assert_eq!(stats.num_rows, Some(1500));

        Ok(())
    }
}
