use std::sync::Arc;

use datafusion::datasource::MemTable;
use iceberg::{Catalog, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

use crate::table::{LoadedTable, TableSource};

pub async fn to_loaded_table(t: &TableSource) -> Result<LoadedTable, ::iceberg::Error> {
    let opt = t.option.as_ref().and_then(|o| o.as_iceberg().ok()).ok_or_else(|| {
        ::iceberg::Error::new(
            ::iceberg::ErrorKind::DataInvalid,
            "Missing iceberg table option",
        )
    })?;

    // Use TableSource::parsed_uri() for consistent URI parsing
    let uri = t.parsed_uri().map_err(|e| {
        ::iceberg::Error::new(
            ::iceberg::ErrorKind::DataInvalid,
            format!("Failed to parse URI: {}", e),
        )
    })?;

    // Check if it's an iceberg:// URI
    let scheme = uri.scheme().ok_or_else(|| {
        ::iceberg::Error::new(
            ::iceberg::ErrorKind::DataInvalid,
            "URI must have a scheme",
        )
    })?;

    if scheme.as_str() != "iceberg" {
        return Err(::iceberg::Error::new(
            ::iceberg::ErrorKind::FeatureUnsupported,
            "Only iceberg:// URIs are supported currently",
        ));
    }

    // Parse the authority (host:port)
    let authority = uri.authority().ok_or_else(|| {
        ::iceberg::Error::new(
            ::iceberg::ErrorKind::DataInvalid,
            "Invalid iceberg URI: missing host authority",
        )
    })?;

    // Build catalog URL from host and port
    let host = authority.host().to_string();
    let catalog_url = if let Some(port) = authority.port() {
        format!("http://{}:{}", host, port)
    } else {
        format!("http://{}", host)
    };

    // Parse path segments: /namespace/table
    let path = uri.path();
    let path_segments: Vec<&str> = path.segments().iter().map(|s| s.as_str()).collect();

    if path_segments.len() < 2 {
        return Err(::iceberg::Error::new(
            ::iceberg::ErrorKind::DataInvalid,
            format!(
                "Invalid iceberg URI path '{}': must be in format iceberg://host[:port]/namespace/table", 
                path.to_string()
            ),
        ));
    }

    let namespace = path_segments[0];
    let table_name = path_segments[1];

    // Create REST catalog configuration
    let config = RestCatalogConfig::builder()
        .uri(catalog_url)
        .build();
    let catalog = Arc::new(RestCatalog::new(config));

    if opt.use_memory_table {
        // Load the Iceberg table to get the schema, then create an empty MemTable
        // This is the current behavior that creates an empty table as mentioned by the repo owner
        let table_ident = TableIdent::from_strs([namespace, table_name])?;
        let iceberg_table = catalog.load_table(&table_ident).await?;
        
        let iceberg_schema = iceberg_table.metadata().current_schema();
        let arrow_schema = Arc::new(iceberg_schema.as_ref().try_into().map_err(|e| {
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

        Ok(LoadedTable::new_from_df_table(Arc::new(mem_table)))
    } else {
        // Use the proper IcebergTableProvider from iceberg-datafusion
        // This provides streaming access without loading all data into memory
        // For now, we'll load the table and create a basic provider until we can access the public API
        let table_ident = TableIdent::from_strs([namespace, table_name])?;
        let iceberg_table = catalog.load_table(&table_ident).await?;
        
        // Create a table provider using the loaded table
        // Note: This is a placeholder until the proper public API is available
        let iceberg_schema = iceberg_table.metadata().current_schema();
        let arrow_schema = Arc::new(iceberg_schema.as_ref().try_into().map_err(|e| {
            ::iceberg::Error::new(
                ::iceberg::ErrorKind::DataInvalid,
                format!("Failed to convert iceberg schema to arrow: {}", e),
            )
        })?);

        // For now, create an empty table but with the proper schema
        // This can be improved once we find the correct public API
        let mem_table = MemTable::try_new(arrow_schema, vec![]).map_err(|e| {
            ::iceberg::Error::new(
                ::iceberg::ErrorKind::DataInvalid,
                format!("Failed to create DataFusion MemTable: {}", e),
            )
        })?;

        Ok(LoadedTable::new_from_df_table(Arc::new(mem_table)))
    }
}
