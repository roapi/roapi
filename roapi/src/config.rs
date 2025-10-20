use columnq::datafusion::config::ConfigOptions;
use columnq::SessionConfig;
use serde_derive::Deserialize;
use snafu::{whatever, Whatever};

use columnq::encoding;
use columnq::table::parse_table_uri_arg;
use columnq::table::KeyValueSource;
use columnq::table::TableSource;
use std::collections::HashMap;
use std::fs;
use std::time::Duration;

#[derive(Deserialize, Default, Clone)]
pub struct AddrConfig {
    pub http: Option<String>,
    pub postgres: Option<String>,
    pub flight_sql: Option<String>,
}

#[derive(Deserialize, Default, Clone)]
pub struct FlightSqlTlsConfig {
    pub cert: String,
    pub key: String,
    pub client_ca: String,
}

#[derive(Deserialize, Default, Clone)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Default, Clone)]
pub struct FlightSqlConfig {
    pub tls: Option<FlightSqlTlsConfig>,
    pub auth_token: Option<String>,
    pub auth_basic: Option<BasicAuth>,
}

#[derive(Deserialize, Default, Clone)]
pub struct Config {
    #[serde(default)]
    pub addr: AddrConfig,
    pub tables: Vec<TableSource>,
    pub reload_interval: Option<Duration>,
    #[serde(default)]
    pub disable_read_only: bool,
    #[serde(default)]
    pub kvstores: Vec<KeyValueSource>,
    #[serde(default)]
    pub response_format: encoding::ContentType,
    #[serde(default)]
    pub datafusion: Option<HashMap<String, String>>,
    #[serde(default)]
    pub flight_sql_config: Option<FlightSqlConfig>,
}

fn table_arg() -> clap::Arg {
    clap::Arg::new("table")
        .help("Table sources to load. Table option can be provided as optional setting as part of the table URI, for example: `blogs=s3://bucket/key,format=delta`. Set table uri to `stdin` if you want to consume table data from stdin as part of a UNIX pipe. If no table_name is provided, a table name will be derived from the filename in URI.")
        .num_args(1)
        .required(false)
        .number_of_values(1)
        .action(clap::ArgAction::Append)
        .value_name("[table_name=]uri[,option_key=option_value]")
        .long("table")
        .short('t')
}

fn address_http_arg() -> clap::Arg {
    clap::Arg::new("addr-http")
        .help("HTTP endpoint bind address")
        .required(false)
        .num_args(1)
        .value_name("IP:PORT")
        .long("addr-http")
        .short('a')
}

fn address_postgres_arg() -> clap::Arg {
    clap::Arg::new("addr-postgres")
        .help("Postgres endpoint bind address")
        .required(false)
        .num_args(1)
        .value_name("IP:PORT")
        .long("addr-postgres")
        .short('p')
}

fn address_flight_sql_arg() -> clap::Arg {
    clap::Arg::new("addr-flight-sql")
        .help("FlightSQL endpoint bind address")
        .required(false)
        .num_args(1)
        .value_name("IP:PORT")
        .long("addr-flight-sql")
}

fn read_only_arg() -> clap::Arg {
    clap::Arg::new("disable-read-only")
        .help("Start roapi in read write mode, allowing tables to be updated at runtime")
        .required(false)
        .num_args(0)
        .long("disable-read-only")
        .short('d')
        .action(clap::ArgAction::SetTrue)
}

fn reload_interval_arg() -> clap::Arg {
    clap::Arg::new("reload-interval")
        .help("maximum age in seconds before triggering rescan and reload of the tables")
        .required(false)
        .num_args(1)
        .long("reload-interval")
        .short('r')
        .value_parser(clap::value_parser!(u64))
}

fn response_format_arg() -> clap::Arg {
    clap::Arg::new("response-format")
        .help("change response serialization: Json (default), Csv, ArrowFile, ArrowStream, Parquet")
        .required(false)
        .num_args(1)
        .value_name("ResponseFormat")
        .long("response-format")
        .short('f')
}

fn config_arg() -> clap::Arg {
    clap::Arg::new("config")
        .help("config file path")
        .required(false)
        .num_args(1)
        .long("config")
        .short('c')
}

pub fn get_cmd() -> clap::Command {
    let features: &[&'static str] = &[
        #[cfg(feature = "database")]
        "database",
        #[cfg(feature = "database-sqlite")]
        "database-sqlite",
        #[cfg(feature = "database-postgres")]
        "database-postgres",
        #[cfg(feature = "database-mysql")]
        "database-mysql",
        #[cfg(feature = "snmalloc")]
        "snmalloc",
        #[cfg(feature = "rustls")]
        "rustls",
        #[cfg(feature = "ui")]
        "ui",
    ];

    clap::Command::new("roapi")
        .version(env!("CARGO_PKG_VERSION"))
        .author("QP Hou")
        .about(
            "Create full-fledged APIs for static datasets without writing a single line of code.",
        )
        .after_help(format!("Built with features: {}", features.join(", ")))
        .arg_required_else_help(true)
        .args(&[
            address_http_arg(),
            address_postgres_arg(),
            address_flight_sql_arg(),
            config_arg(),
            read_only_arg(),
            reload_interval_arg(),
            response_format_arg(),
            table_arg(),
        ])
}

pub fn get_configuration(cmd: clap::Command) -> Result<Config, Whatever> {
    let matches = cmd.get_matches();

    let mut config: Config = match matches.get_one::<String>("config") {
        None => Config::default(),
        Some(config_path) => {
            let config_content = whatever!(
                fs::read_to_string(config_path),
                "Failed to read config file: {config_path}",
            );
            if config_path.ends_with(".yaml") || config_path.ends_with(".yml") {
                whatever!(
                    serde_yaml::from_str(&config_content),
                    "Failed to parse YAML config",
                )
            } else if config_path.ends_with(".toml") {
                whatever!(
                    toml::from_str(&config_content),
                    "Failed to parse TOML config"
                )
            } else {
                whatever!("Unsupported config file format: {}", config_path);
            }
        }
    };

    if let Some(tables) = matches.get_many::<String>("table") {
        for v in tables {
            config.tables.push(whatever!(
                parse_table_uri_arg(v),
                "Failed to parse table uri: {v}"
            ));
        }
    }

    if let Some(addr) = matches.get_one::<String>("addr-http") {
        config.addr.http = Some(addr.clone());
    }

    if let Some(addr) = matches.get_one::<String>("addr-postgres") {
        config.addr.postgres = Some(addr.clone());
    }

    if let Some(addr) = matches.get_one::<String>("addr-flight-sql") {
        config.addr.flight_sql = Some(addr.clone());
    }

    if matches.get_one::<bool>("disable-read-only") == Some(&true) {
        config.disable_read_only = true;
    }

    if let Some(reload_interval) = matches.get_one::<u64>("reload-interval") {
        if !config.disable_read_only {
            whatever!("Table reload not supported in read-only mode. Try specify the --disable-read-only option.");
        }
        config.reload_interval = Some(Duration::from_secs(*reload_interval));
    }

    if let Some(response_format) = matches.get_one::<String>("response-format") {
        config.response_format = whatever!(
            serde_yaml::from_str(response_format),
            "Failed parse response-format",
        );
    }

    Ok(config)
}

impl Config {
    pub fn get_datafusion_config(&self) -> Result<SessionConfig, Whatever> {
        match &self.datafusion {
            Some(df_cfg) => {
                let mut opt = ConfigOptions::default();
                for (k, v) in df_cfg {
                    whatever!(
                        opt.set(format!("datafusion.{k}").as_str(), v),
                        "failed to set datafusion config: {k}={v}"
                    );
                }
                Ok(opt.into())
            }
            None => Ok(SessionConfig::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_read_only() {
        let matches = get_cmd()
            .try_get_matches_from(vec!["roapi", "--disable-read-only", "-c", "a.yml"])
            .unwrap();
        assert!(matches.get_one::<bool>("disable-read-only").unwrap());

        let matches = get_cmd()
            .try_get_matches_from(vec!["roapi", "-c", "a.yml"])
            .unwrap();
        assert_eq!(matches.get_one::<bool>("disable-read-only"), Some(&false));
    }

    #[test]
    fn test_parse_reload_interval() {
        let matches = get_cmd()
            .try_get_matches_from(vec!["roapi", "--reload-interval", "10", "-c", "a.yml"])
            .unwrap();
        assert_eq!(matches.get_one::<u64>("reload-interval"), Some(&10));
    }
}
