use std::{collections::HashMap, sync::Arc};

use axum::extract::{Extension, Json};
use columnq::table::TableSource;
use log::info;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::context::RoapiContext;
use crate::error::ApiErrResp;

#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    #[serde(rename = "tableName")]
    pub table_name: String,
}

pub async fn drop_table<H: RoapiContext>(
    Extension(tables): Extension<Arc<Mutex<HashMap<String, TableSource>>>>,
    Json(body): Json<Vec<SourceConfig>>,
) -> Result<(), ApiErrResp> {
    let mut tables = tables.lock().await;
    for config in body {
        if tables.contains_key(&config.table_name) {
            info!("dropping table `{}`", config.table_name);
            tables.remove(&config.table_name);
            info!("dropped table `{}`", config.table_name);
        } else {
            return Err(ApiErrResp::register_table(format!(
                "Table `{}` source does not exist",
                config.table_name
            )));
        }
    }
    Ok(())
}

pub async fn drop_table_read_only() -> Result<(), ApiErrResp> {
    Err(ApiErrResp::read_only_mode())
}
