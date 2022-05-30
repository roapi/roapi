use std::{collections::HashMap, sync::Arc};

use axum::extract::{Extension, Json};
use columnq::{error::ColumnQError, table::TableSource};
use log::info;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::context::RoapiContext;
use crate::error::ApiErrResp;

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    #[serde(rename = "tableName")]
    pub table_name: String,
    pub uri: Option<String>,
}

pub async fn register_table<H: RoapiContext>(
    Extension(ctx): Extension<Arc<H>>,
    Extension(tables): Extension<Arc<Mutex<HashMap<String, TableSource>>>>,
    Json(body): Json<Vec<SourceConfig>>,
) -> Result<(), ApiErrResp> {
    let mut tables = tables.lock().await;
    for config in body {
        if let Some(ref uri) = config.uri {
            let t = TableSource::new_with_uri(&config.table_name, uri);
            info!("loading `{}` as table `{}`", t.io_source, config.table_name);
            ctx.load_table(&t)
                .await
                .map_err(ColumnQError::from)
                .map_err(ApiErrResp::load_table)?;
            tables.insert(config.table_name.clone(), t.clone());
            info!(
                "registered `{}` as table `{}`",
                t.io_source, config.table_name
            );
        } else if let Some(t) = tables.get(&config.table_name) {
            info!("Re register table {}", t.name);
            ctx.load_table(t)
                .await
                .map_err(ColumnQError::from)
                .map_err(ApiErrResp::load_table)?;
        } else {
            return Err(ApiErrResp::register_table(format!(
                "Table `{}` source not exists",
                config.table_name
            )));
        }
    }
    Ok(())
}

pub async fn register_table_read_only() -> Result<(), ApiErrResp> {
    Err(ApiErrResp::read_only_mode())
}
