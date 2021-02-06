use serde_derive::Deserialize;

use columnq::table::TableSource;

#[derive(Deserialize, Default)]
pub struct Config {
    pub addr: Option<String>,
    pub tables: Vec<TableSource>,
}
