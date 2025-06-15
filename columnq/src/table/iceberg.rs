use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::MemTable;
use iceberg::{Catalog, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

use crate::table::{LoadedTable, TableSource};

pub async fn to_loaded_table(t: &TableSource) -> Result<LoadedTable, ::iceberg::Error> {
    let uri = t.get_uri_str();

    if let Some(catalog_path) = uri.strip_prefix("iceberg://") {
        let mut parts = catalog_path.splitn(3, '/');
        let host = parts.next().ok_or_else(|| {
            ::iceberg::Error::new(
                ::iceberg::ErrorKind::DataInvalid,
                "Invalid iceberg URI: missing host",
            )
        })?;
        let namespace = parts.next().ok_or_else(|| {
            ::iceberg::Error::new(
                ::iceberg::ErrorKind::DataInvalid,
                "Invalid iceberg URI: missing namespace",
            )
        })?;
        let table_name = parts.next().ok_or_else(|| {
            ::iceberg::Error::new(
                ::iceberg::ErrorKind::DataInvalid,
                "Invalid iceberg URI: missing table name",
            )
        })?;

        let config = RestCatalogConfig::builder()
            .uri(format!("http://{host}"))
            .build();
        let catalog = RestCatalog::new(config);

        let table_ident = TableIdent::from_strs([namespace, table_name])?;
        let table = catalog.load_table(&table_ident).await?;

        let iceberg_schema = table.metadata().current_schema();
        let arrow_schema: SchemaRef =
            Arc::new(iceberg_schema.as_ref().try_into().map_err(|e| {
                ::iceberg::Error::new(
                    ::iceberg::ErrorKind::DataInvalid,
                    format!("Failed to convert iceberg schema to arrow: {}", e),
                )
            })?);

        let mem_table = MemTable::try_new(arrow_schema, vec![]).map_err(|e| {
            ::iceberg::Error::new(
                ::iceberg::ErrorKind::DataInvalid,
                format!("Failed to create DataFusion MemTable: {}", e),
            )
        })?;

        let loaded_table = LoadedTable::new_from_df_table(Arc::new(mem_table));
        Ok(loaded_table)
    } else {
        Err(::iceberg::Error::new(
            ::iceberg::ErrorKind::FeatureUnsupported,
            "Only iceberg REST catalog is supported currently. URI must start with iceberg://",
        ))
    }
}
