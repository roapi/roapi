use serde_derive::Deserialize;

use anyhow::{Context, Result};

use columnq::table::parse_table_uri_arg;
use columnq::table::TableSource;
use std::fs;

#[derive(Deserialize, Default)]
pub struct Config {
    pub addr: Option<String>,
    pub tables: Vec<TableSource>,
}

fn table_arg() -> clap::Arg<'static> {
    clap::Arg::new("table")
        .about("Table sources to load. Table option can be provided as optional setting as part of the table URI, for example: `blogs=s3://bucket/key,format=delta`. Set table uri to `stdin` if you want to consume table data from stdin as part of a UNIX pipe. If no table_name is provided, a table name will be derived from the filename in URI.")
        .takes_value(true)
        .required(false)
        .number_of_values(1)
        .multiple_occurrences(true)
        .value_name("[table_name=]uri[,option_key=option_value]")
        .long("table")
        .short('t')
}

fn address_arg() -> clap::Arg<'static> {
    clap::Arg::new("addr")
        .about("bind address")
        .required(false)
        .takes_value(true)
        .value_name("IP:PORT")
        .long("addr")
        .short('a')
}

fn config_arg() -> clap::Arg<'static> {
    clap::Arg::new("config")
        .about("config file path")
        .required(false)
        .takes_value(true)
        .long("config")
        .short('c')
}

pub fn get_configuration() -> Result<Config, anyhow::Error> {
    let matches = clap::App::new("roapi-http")
        .version(env!("CARGO_PKG_VERSION"))
        .author("QP Hou")
        .about(
            "Create full-fledged APIs for static datasets without writing a single line of code.",
        )
        .setting(clap::AppSettings::ArgRequiredElseHelp)
        .args(&[address_arg(), config_arg(), table_arg()])
        .get_matches();

    let mut config: Config = match matches.value_of("config") {
        None => Config::default(),
        Some(config_path) => {
            let config_content = fs::read_to_string(config_path)
                .with_context(|| format!("Failed to read config file: {}", config_path))?;

            serde_yaml::from_str(&config_content).context("Failed to parse YAML config")?
        }
    };

    if let Some(tables) = matches.values_of("table") {
        for v in tables {
            config.tables.push(parse_table_uri_arg(v)?);
        }
    }

    if let Some(addr) = matches.value_of("addr") {
        config.addr = Some(addr.to_string());
    }

    Ok(config)
}
