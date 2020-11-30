use serde_derive::Deserialize;

use crate::table::TableSource;

#[derive(Deserialize)]
pub struct Config {
    pub addr: Option<String>,
    pub tables: Vec<TableSource>,
}
