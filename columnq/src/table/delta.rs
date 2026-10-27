use snafu::prelude::*;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::physical_plan::collect;
use deltalake::DeltaTableBuilder;
use object_store::DynObjectStore;

use crate::io::{self, BlobStoreType};
use crate::table::{self, LoadedTable, TableLoadOption, TableOptionDelta, TableSource};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Scheme in table uri not supported for delta table: {uri}"))]
    InvalidUri { uri: String },
    #[snafu(display("Failed to open table: {source}"))]
    OpenTable {
        source: deltalake::errors::DeltaTableError,
    },
    #[snafu(display("Failed to load table: {source}"))]
    LoadTable {
        source: deltalake::errors::DeltaTableError,
    },
    #[snafu(display("Failed to update table: {source}"))]
    UpdateTable {
        source: deltalake::errors::DeltaTableError,
    },
    #[snafu(display("Failed to create memory table: {source}"))]
    CreateMemTable {
        source: datafusion::error::DataFusionError,
    },
}

async fn update_table(
    mut t: deltalake::DeltaTable,
) -> Result<Arc<dyn TableProvider>, table::Error> {
    t = t
        .update()
        .await
        .map(|(t, _)| t)
        .context(UpdateTableSnafu)
        .map_err(Box::new)
        .context(table::LoadDeltaSnafu)?;
    t.table_provider()
        .await
        .map_err(|e| table::Error::LoadDelta {
            source: Box::new(Error::UpdateTable {
                source: deltalake::errors::DeltaTableError::Generic(e.to_string()),
            }),
        })
}

/// Resolve the table URL and look up the object store that was already
/// registered in DataFusion's runtime env.  Returns `None` for local
/// filesystem paths (no store registration needed).
fn get_registered_store(
    uri_str: &str,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<Option<(url::Url, Arc<DynObjectStore>)>, table::Error> {
    if !uri_str.contains("://") {
        // local filesystem – no registered store
        return Ok(None);
    }
    let url = url::Url::parse(uri_str).map_err(|_| table::Error::LoadDelta {
        source: Box::new(Error::OpenTable {
            source: deltalake::errors::DeltaTableError::InvalidTableLocation(uri_str.to_string()),
        }),
    })?;
    match dfctx.runtime_env().object_store_registry.get_store(&url) {
        Ok(store) => Ok(Some((url, store))),
        // If not registered yet, fall back to delta-rs default resolution
        Err(_) => Ok(None),
    }
}

pub async fn to_loaded_table(
    t: &TableSource,
    dfctx: &datafusion::execution::context::SessionContext,
) -> Result<LoadedTable, table::Error> {
    let opt = t
        .option
        .clone()
        .unwrap_or_else(|| TableLoadOption::delta(TableOptionDelta::default()));

    let TableOptionDelta { use_memory_table } = opt.as_delta()?;

    let uri_str = t.get_uri_str();

    // Build the URL (handle both remote URIs and bare filesystem paths).
    let url = if uri_str.contains("://") {
        url::Url::parse(uri_str).ok()
    } else {
        let abs_path =
            std::fs::canonicalize(uri_str).unwrap_or_else(|_| std::path::PathBuf::from(uri_str));
        url::Url::from_file_path(abs_path).ok()
    }
    .ok_or_else(|| table::Error::LoadDelta {
        source: Box::new(Error::OpenTable {
            source: deltalake::errors::DeltaTableError::InvalidTableLocation(uri_str.to_string()),
        }),
    })?;

    // Reuse the object store that is already registered in DataFusion's
    // runtime env so that settings like `allow_http = true` apply uniformly
    // to both DataFusion scans and delta-rs metadata reads.
    let delta_table = match get_registered_store(uri_str, dfctx)? {
        Some((store_url, store)) => DeltaTableBuilder::from_url(url.clone())
            .context(OpenTableSnafu)
            .map_err(Box::new)
            .context(table::LoadDeltaSnafu)?
            .with_storage_backend(store, store_url)
            .load()
            .await
            .context(OpenTableSnafu)
            .map_err(Box::new)
            .context(table::LoadDeltaSnafu)?,
        None => deltalake::open_table(url.clone())
            .await
            .context(OpenTableSnafu)
            .map_err(Box::new)
            .context(table::LoadDeltaSnafu)?,
    };

    let parsed_uri = t.parsed_uri()?;
    let url_scheme = parsed_uri.scheme();
    let blob_type = BlobStoreType::try_from(url_scheme).context(table::IoSnafu)?;
    let batch_size = t.batch_size;

    if *use_memory_table {
        let dfctx = dfctx.clone();
        let to_datafusion_table = move || {
            to_mem_table(
                delta_table.clone(),
                blob_type.clone(),
                batch_size,
                dfctx.clone(),
            )
        };
        LoadedTable::new_from_df_table_cb(to_datafusion_table).await
    } else {
        let curr_table = delta_table.clone();
        let df_table = cast_datafusion_table(delta_table, blob_type).await?;
        Ok(LoadedTable::new(
            df_table,
            Box::new(move || {
                let next_table = curr_table.clone();
                Box::pin(update_table(next_table))
            }),
        ))
    }
}

async fn cast_datafusion_table(
    delta_table: deltalake::DeltaTable,
    blob_type: io::BlobStoreType,
) -> Result<Arc<dyn TableProvider>, table::Error> {
    match blob_type {
        io::BlobStoreType::Azure
        | io::BlobStoreType::S3
        | io::BlobStoreType::GCS
        | io::BlobStoreType::FileSystem => {
            Ok(delta_table
                .table_provider()
                .await
                .map_err(|e| table::Error::LoadDelta {
                    source: Box::new(Error::LoadTable {
                        source: deltalake::errors::DeltaTableError::Generic(e.to_string()),
                    }),
                })?)
        }
        _ => Err(Box::new(Error::InvalidUri {
            uri: delta_table.table_url().to_string(),
        }))
        .context(table::LoadDeltaSnafu),
    }
}

/// Materialize a Delta table into an in-memory MemTable.
///
/// Uses Delta's DataFusion [`TableProvider`] so partition columns from table
/// metadata are included. Reading the underlying parquet files alone omits
/// those columns and then fails `MemTable::try_new` against the Delta schema
/// (see #408).
pub async fn to_mem_table(
    delta_table: deltalake::DeltaTable,
    blob_type: io::BlobStoreType,
    _batch_size: usize,
    dfctx: datafusion::execution::context::SessionContext,
) -> Result<Arc<dyn TableProvider>, table::Error> {
    let provider = cast_datafusion_table(delta_table, blob_type).await?;
    let schema = provider.schema();

    let exec = provider
        .scan(&dfctx.state(), None, &[], None)
        .await
        .map_err(|e| table::Error::LoadDelta {
            source: Box::new(Error::CreateMemTable { source: e }),
        })?;

    let batches = collect(exec, dfctx.task_ctx())
        .await
        .map_err(|e| table::Error::LoadDelta {
            source: Box::new(Error::CreateMemTable { source: e }),
        })?;

    let partitions = if batches.is_empty() {
        vec![vec![]]
    } else {
        vec![batches]
    };

    let mem_table = datafusion::datasource::MemTable::try_new(schema, partitions).map_err(|e| {
        table::Error::LoadDelta {
            source: Box::new(Error::CreateMemTable { source: e }),
        }
    })?;

    Ok(Arc::new(mem_table))
}

#[cfg(test)]
mod tests {

    use super::*;
    use datafusion::common::stats::Precision;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::Statistics;
    use datafusion::prelude::SessionContext;

    use crate::test_util::test_data_path;

    #[tokio::test]
    async fn load_delta_as_memtable() {
        let ctx = SessionContext::new();
        let t = to_loaded_table(
            &TableSource::new("blogs".to_string(), test_data_path("blogs-delta")).with_option(
                TableLoadOption::delta(TableOptionDelta {
                    use_memory_table: true,
                }),
            ),
            &ctx,
        )
        .await
        .unwrap()
        .table;

        validate_statistics(
            t.scan(&ctx.state(), None, &[], None)
                .await
                .unwrap()
                .partition_statistics(None)
                .unwrap(),
        );

        if t.as_any().downcast_ref::<MemTable>().is_none() {
            panic!("must be of type datafusion::datasource::MemTable");
        }
    }

    #[tokio::test]
    async fn load_delta_as_delta_source() {
        let ctx = SessionContext::new();
        let _t = to_loaded_table(
            &TableSource::new("blogs".to_string(), test_data_path("blogs-delta")).with_option(
                TableLoadOption::delta(TableOptionDelta {
                    use_memory_table: false,
                }),
            ),
            &ctx,
        )
        .await
        .unwrap()
        .table;
    }

    #[tokio::test]
    async fn load_partitioned_delta_as_memtable() {
        let ctx = SessionContext::new();
        let t = to_loaded_table(
            &TableSource::new(
                "partitioned".to_string(),
                test_data_path("partitioned-delta"),
            )
            .with_option(TableLoadOption::delta(TableOptionDelta {
                use_memory_table: true,
            })),
            &ctx,
        )
        .await
        .unwrap()
        .table;

        assert!(
            t.as_any().downcast_ref::<MemTable>().is_some(),
            "must be of type datafusion::datasource::MemTable"
        );

        let schema = t.schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            names.contains(&"when"),
            "partition column `when` must be present in MemTable schema, got {names:?}"
        );
        assert!(
            names.contains(&"id"),
            "data column `id` must be present in MemTable schema, got {names:?}"
        );

        let batches = collect(
            t.scan(&ctx.state(), None, &[], None).await.unwrap(),
            ctx.task_ctx(),
        )
        .await
        .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3);

        // Partition values must be present (not dropped when materializing).
        let when_idx = schema.index_of("when").unwrap();
        let nulls: usize = batches
            .iter()
            .map(|b| b.column(when_idx).null_count())
            .sum();
        assert_eq!(nulls, 0, "partition column `when` must be populated");
    }

    fn validate_statistics(stats: Statistics) {
        assert_eq!(stats.num_rows, Precision::Exact(500));
        let column_stats = stats.column_statistics;
        assert_eq!(column_stats[0].null_count, Precision::Exact(245));
        assert_eq!(column_stats[1].null_count, Precision::Exact(373));
        assert_eq!(column_stats[2].null_count, Precision::Exact(237));
    }
}
