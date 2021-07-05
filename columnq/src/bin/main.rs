use arrow::util::pretty;
use rustyline::error::ReadlineError;
use rustyline::Editor;

use columnq::table::TableSource;
use columnq::{ColumnQ, ExecutionConfig};

async fn console_loop(cq: &ColumnQ) -> anyhow::Result<()> {
    let mut rl = Editor::<()>::new();
    // if rl.load_history("history.txt").is_err() {
    //     println!("No previous history.");
    // }
    loop {
        let readline = rl.readline("columnq(sql)> ");
        match readline {
            Ok(line) => {
                // rl.add_history_entry(line.as_str());
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
    // rl.save_history("history.txt").unwrap();

    Ok(())
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
        .about("ETL for local datasets the Unix way.")
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
