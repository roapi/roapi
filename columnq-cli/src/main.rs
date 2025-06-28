use anyhow::{anyhow, Context};
use log::debug;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::io::Write;
use std::path::PathBuf;

use columnq::datafusion::arrow::util::pretty;
use columnq::table::parse_table_uri_arg;
use columnq::{encoding, ColumnQ, SessionConfig};

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

fn history_path() -> anyhow::Result<PathBuf> {
    let mut home =
        dirs::home_dir().ok_or_else(|| anyhow!("Failed to locate user home directory"))?;
    home.push(".columnq_history");
    Ok(home)
}

#[allow(dead_code)]
fn config_path() -> anyhow::Result<PathBuf> {
    let mut home =
        dirs::home_dir().ok_or_else(|| anyhow!("Failed to locate user home directory"))?;

    home.push(".config");
    home.push("columnq");

    Ok(home)
}

fn table_arg() -> clap::Arg {
    clap::Arg::new("table")
        .long_help("Table sources to load. Table option can be provided as optional setting as part of the table URI, for example: `blogs=s3://bucket/key,format=delta`. Set table uri to `stdin` if you want to consume table data from stdin as part of a UNIX pipe. If no table_name is provided, a table name will be derived from the filename in URI.")
        .num_args(1)
        .required(false)
        .number_of_values(1)
        .action(clap::ArgAction::Append)
        .value_name("[table_name=]uri[,option_key=option_value]")
        .long("table")
        .short('t')
}

async fn console_loop(cq: &ColumnQ) -> anyhow::Result<()> {
    let rl_history = history_path()?;

    let mut readline = Editor::<()>::new();
    if let Err(e) = readline.load_history(&rl_history) {
        debug!("no query history loaded: {e:?}");
    }

    loop {
        match readline.readline("columnq(sql)> ") {
            Ok(line) => {
                readline.add_history_entry(line.as_str());
                match line.as_ref() {
                    "exit" | "quit" | "q" => {
                        println!("Good bye!");
                        break;
                    }
                    s => match cq.query_sql(s).await {
                        Ok(batches) => {
                            pretty::print_batches(&batches)?;
                        }
                        Err(e) => {
                            println!("Error: {e}");
                        }
                    },
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                println!("Good bye!");
                break;
            }
            Err(err) => {
                println!("Error: {err:?}");
                break;
            }
        }
    }

    readline
        .save_history(&rl_history)
        .context("Failed to save query history")
}

async fn cmd_console(args: &clap::ArgMatches) -> anyhow::Result<()> {
    let config = SessionConfig::default().with_information_schema(true);
    let mut cq = ColumnQ::new_with_config(config, true, None);

    if let Some(tables) = args.get_many::<String>("table") {
        for v in tables {
            cq.load_table(&parse_table_uri_arg(v)?).await?;
        }
    }

    console_loop(&cq).await
}

fn bytes_to_stdout(bytes: &[u8]) -> anyhow::Result<()> {
    let mut out = std::io::stdout();
    out.write_all(bytes)?;
    out.flush()?;
    Ok(())
}

async fn cmd_sql(args: &clap::ArgMatches) -> anyhow::Result<()> {
    let config = SessionConfig::default().with_information_schema(true);
    let mut cq = ColumnQ::new_with_config(config, true, None);

    if let Some(tables) = args.get_many::<String>("table") {
        for v in tables {
            cq.load_table(&parse_table_uri_arg(v)?).await?;
        }
    }

    match args.get_one::<String>("SQL") {
        Some(query) => match cq.query_sql(query).await {
            Ok(batches) => match args
                .get_one::<String>("output")
                .unwrap_or(&String::from("table"))
                .as_str()
            {
                "table" => pretty::print_batches(&batches)?,
                "json" => {
                    let bytes = encoding::json::record_batches_to_bytes(&batches)?;
                    bytes_to_stdout(&bytes)?;
                }
                "csv" => {
                    let bytes = encoding::csv::record_batches_to_bytes(&batches)?;
                    bytes_to_stdout(&bytes)?;
                }
                "parquet" => {
                    let bytes = encoding::parquet::record_batches_to_bytes(&batches)?;
                    bytes_to_stdout(&bytes)?;
                }
                "arrow" => {
                    let bytes = encoding::arrow::record_batches_to_file_bytes(&batches)?;
                    bytes_to_stdout(&bytes)?;
                }
                "arrows" => {
                    let bytes = encoding::arrow::record_batches_to_stream_bytes(&batches)?;
                    bytes_to_stdout(&bytes)?;
                }
                other => anyhow::bail!("unsupported output format: {}", other),
            },
            Err(e) => {
                println!("Error: {e}");
            }
        },
        None => {
            unreachable!();
        }
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let app = clap::Command::new("Columnq")
        .version(env!("CARGO_PKG_VERSION"))
        .author("QP Hou")
        .about("OLAP the Unix way.")
        .arg_required_else_help(true)
        .subcommand_required(true)
        // .setting(clap::Command::SubcommandRequiredElseHelp)
        // .setting(clap::Command::ar)
        .subcommand(
            clap::Command::new("sql")
                .about("Query tables with SQL")
                .arg_required_else_help(true)
                .args(&[
                    clap::Arg::new("SQL")
                        .help("SQL query to execute")
                        .index(1)
                        .required(true)
                        .num_args(1),
                    clap::Arg::new("output")
                        .help("Query output format")
                        .long("output")
                        .short('o')
                        .required(false)
                        .num_args(1)
                        .default_value("table")
                        // TODO: add yaml
                        .value_parser(["table", "json", "csv", "parquet", "arrow", "arrows"]),
                    table_arg(),
                ]),
        )
        .subcommand(
            clap::Command::new("console")
                .about("Query tables through an interactive console")
                .arg_required_else_help(true)
                .args(&[table_arg()]),
        );
    let matches = app.get_matches();

    // let mut path = config_path()?;
    // if !path.as_path().exists() {
    //     std::fs::create_dir(path.as_path())
    //         .with_context(|| format!("Failed to create columnq config directory: {:?}", path))?;
    // }

    match matches.subcommand() {
        Some(("console", console_matches)) => cmd_console(console_matches).await?,
        Some(("sql", console_matches)) => cmd_sql(console_matches).await?,
        _ => unreachable!(),
    }

    Ok(())
}
