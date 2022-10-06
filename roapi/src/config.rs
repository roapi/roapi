use serde_derive::Deserialize;

use anyhow::{Context, Result};

use columnq::table::parse_table_uri_arg;
use columnq::table::KeyValueSource;
use columnq::table::TableSource;
use std::fs;
use std::time::Duration;

#[derive(Deserialize, Default, Clone)]
pub struct AddrConfig {
    pub http: Option<String>,
    pub postgres: Option<String>,
}

#[derive(Deserialize, Default, Clone)]
pub struct Config {
    pub addr: AddrConfig,
    pub tables: Vec<TableSource>,
    pub max_age: Option<Duration>,
    #[serde(default)]
    pub disable_read_only: bool,
    #[serde(default)]
    pub kvstores: Vec<KeyValueSource>,
}

fn table_arg() -> clap::Arg<'static> {
    clap::Arg::new("table")
        .help("Table sources to load. Table option can be provided as optional setting as part of the table URI, for example: `blogs=s3://bucket/key,format=delta`. Set table uri to `stdin` if you want to consume table data from stdin as part of a UNIX pipe. If no table_name is provided, a table name will be derived from the filename in URI.")
        .takes_value(true)
        .required(false)
        .number_of_values(1)
        .multiple_occurrences(true)
        .value_name("[table_name=]uri[,option_key=option_value]")
        .long("table")
        .short('t')
}

fn address_http_arg() -> clap::Arg<'static> {
    clap::Arg::new("addr-http")
        .help("HTTP endpoint bind address")
        .required(false)
        .takes_value(true)
        .value_name("IP:PORT")
        .long("addr-http")
        .short('a')
}

fn address_postgres_arg() -> clap::Arg<'static> {
    clap::Arg::new("addr-postgres")
        .help("Postgres endpoint bind address")
        .required(false)
        .takes_value(true)
        .value_name("IP:PORT")
        .long("addr-postgres")
        .short('p')
}

fn read_only_arg() -> clap::Arg<'static> {
    clap::Arg::new("disable-read-only")
        .help("Start roapi in read write mode")
        .required(false)
        .takes_value(false)
        .long("disable-read-only")
        .short('d')
}

fn max_age_arg() -> clap::Arg<'static> {
    clap::Arg::new("max-age")
        .help("maximum age in seconds before triggering rescan and reload of the files")
        .required(false)
        .takes_value(true)
        .long("max-age")
        .short('m')
}

fn config_arg() -> clap::Arg<'static> {
    clap::Arg::new("config")
        .help("config file path")
        .required(false)
        .takes_value(true)
        .long("config")
        .short('c')
}

pub fn get_configuration() -> Result<Config, anyhow::Error> {
    let matches = clap::Command::new("roapi")
        .version(env!("CARGO_PKG_VERSION"))
        .author("QP Hou")
        .about(
            "Create full-fledged APIs for static datasets without writing a single line of code.",
        )
        .arg_required_else_help(true)
        .args(&[
            address_http_arg(),
            address_postgres_arg(),
            config_arg(),
            read_only_arg(),
            max_age_arg(),
            table_arg(),
        ])
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

    if let Some(addr) = matches.value_of("addr-http") {
        config.addr.http = Some(addr.to_string());
    }

    if let Some(addr) = matches.value_of("addr-postgres") {
        config.addr.postgres = Some(addr.to_string());
    }

    if matches.is_present("disable-read-only") {
        config.disable_read_only = true;
    }

    if let Some(max_age) = matches.value_of("max-age") {
        config.max_age = Some(Duration::from_secs(max_age.to_string().parse().unwrap()));
    }

    Ok(config)
}
