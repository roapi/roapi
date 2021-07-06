use anyhow::{anyhow, Context};
use arrow::util::pretty;
use log::debug;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::path::PathBuf;

use columnq::table::TableSource;
use columnq::{ColumnQ, ExecutionConfig};

fn config_path() -> anyhow::Result<PathBuf> {
    let mut home =
        dirs::home_dir().ok_or_else(|| anyhow!("Failed to locate user home directory"))?;

    home.push(".config");
    home.push("columnq");

    Ok(home)
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
            let mut split = v.splitn(2, ':');
            let table_name = split
                .next()
                .ok_or_else(|| anyhow::anyhow!("invalid table config: {}", v))?;
            let uri = split
                .next()
                .ok_or_else(|| anyhow::anyhow!("invalid table config: {}", v))?;

            let t = TableSource::new(table_name.to_string(), uri.to_string());
            cq.load_table(&t).await?;
        }
    }

    console_loop(&cq).await
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
            clap::App::new("console")
                .about("dump table metadata info")
                .setting(clap::AppSettings::ArgRequiredElseHelp)
                .args(&[clap::Arg::new("table")
                    .about("table sources to load")
                    .takes_value(true)
                    .required(false)
                    .number_of_values(1)
                    .multiple(true)
                    .value_name("table_name:uri")
                    .long("table")
                    .short('t')]),
        );
    let matches = app.get_matches();

    match matches.subcommand() {
        Some(("console", console_matches)) => cmd_console(console_matches).await?,
        _ => unreachable!(),
    }

    Ok(())
}
