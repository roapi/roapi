use anyhow::{anyhow, Context};
use arrow::util::pretty;
use log::debug;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::path::PathBuf;

use columnq::table::parse_table_uri_arg;
use columnq::{encoding, ColumnQ, ExecutionConfig};

fn config_path() -> anyhow::Result<PathBuf> {
    let mut home =
        dirs::home_dir().ok_or_else(|| anyhow!("Failed to locate user home directory"))?;

    home.push(".config");
    home.push("columnq");

    Ok(home)
}

fn table_arg() -> clap::Arg<'static> {
    clap::Arg::new("table")
        .about("Table sources to load. Table option can be provided as optional setting as part of the table URI, for example: `blogs=s3://bucket/key,format=delta`. Set table uri to `stdin` if you want to consume table data from stdin as part of a UNIX pipe. If no table_name is provided, a table name will be derived from the filename in URI.")
        .takes_value(true)
        .required(false)
        .number_of_values(1)
        .multiple(true)
        .value_name("[table_name=]uri[,option_key=option_value]")
        .long("table")
        .short('t')
}

async fn console_loop(cq: &ColumnQ) -> anyhow::Result<()> {
    let mut path = config_path()?;
    if !path.as_path().exists() {
        std::fs::create_dir(path.as_path())
            .with_context(|| format!("Failed to create columnq config directory: {:?}", path))?;
    }

    path.push("history.txt");
    let rl_history = path.as_path();

    let mut readline = Editor::<()>::new();
    if let Err(e) = readline.load_history(rl_history) {
        debug!("no query history loaded: {:?}", e);
    }

    loop {
        match readline.readline("columnq(sql)> ") {
            Ok(line) => {
                readline.add_history_entry(line.as_str());
                match cq.query_sql(&line).await {
                    Ok(batches) => {
                        pretty::print_batches(&batches)?;
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Good bye!");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("Good bye!");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    readline
        .save_history(rl_history)
        .context("Failed to save query history")
}

async fn cmd_console(args: &clap::ArgMatches) -> anyhow::Result<()> {
    let config = ExecutionConfig::default().with_information_schema(true);
    let mut cq = ColumnQ::new_with_config(config);

    if let Some(tables) = args.values_of("table") {
        for v in tables {
            cq.load_table(&parse_table_uri_arg(v)?).await?;
        }
    }

    console_loop(&cq).await
}

async fn cmd_sql(args: &clap::ArgMatches) -> anyhow::Result<()> {
    let config = ExecutionConfig::default().with_information_schema(true);
    let mut cq = ColumnQ::new_with_config(config);

    if let Some(tables) = args.values_of("table") {
        for v in tables {
            cq.load_table(&parse_table_uri_arg(v)?).await?;
        }
    }

    match args.value_of("SQL") {
        Some(query) => match cq.query_sql(&query).await {
            Ok(batches) => match args.value_of("output").unwrap_or("table") {
                "table" => pretty::print_batches(&batches)?,
                "json" => {
                    let bytes = encoding::json::record_batches_to_bytes(&batches)?;
                    println!("{}", String::from_utf8(bytes)?);
                }
                "csv" => {
                    let bytes = encoding::csv::record_batches_to_bytes(&batches)?;
                    println!("{}", String::from_utf8(bytes)?);
                }
                other => anyhow::bail!("unsupported output format: {}", other),
            },
            Err(e) => {
                println!("Error: {}", e);
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

    let app = clap::App::new("Columnq")
        .version("0.0.1")
        .about("OLTP the Unix way.")
        .setting(clap::AppSettings::SubcommandRequiredElseHelp)
        .setting(clap::AppSettings::VersionlessSubcommands)
        .subcommand(
            clap::App::new("sql")
                .about("Query tables with SQL")
                .setting(clap::AppSettings::ArgRequiredElseHelp)
                .args(&[
                    clap::Arg::new("SQL")
                        .about("SQL query to execute")
                        .index(1)
                        .required(true)
                        .takes_value(true)
                        .number_of_values(1),
                    clap::Arg::new("output")
                        .about("Query output format")
                        .long("output")
                        .short('o')
                        .required(false)
                        .takes_value(true)
                        .number_of_values(1)
                        .default_value("table")
                        // TODO: add yaml
                        .possible_values(&["table", "json", "csv"]),
                    table_arg(),
                ]),
        )
        .subcommand(
            clap::App::new("console")
                .about("Query tables through an interactive console")
                .setting(clap::AppSettings::ArgRequiredElseHelp)
                .args(&[table_arg()]),
        );
    let matches = app.get_matches();

    match matches.subcommand() {
        Some(("console", console_matches)) => cmd_console(console_matches).await?,
        Some(("sql", console_matches)) => cmd_sql(console_matches).await?,
        _ => unreachable!(),
    }

    Ok(())
}
