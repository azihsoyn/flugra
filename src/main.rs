mod cli;
mod discovery;
mod executor;
mod hooks;
mod ledger;
mod migration_parser;
mod parser;
mod planner;
mod schema;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};

#[tokio::main]
async fn main() -> Result<()> {
    // Show banner when --help is requested
    let args: Vec<String> = std::env::args().collect();
    if args.len() <= 1 || args.iter().any(|a| a == "--help" || a == "-h") {
        eprint!("{}", cli::banner());
    }

    let cli = Cli::parse();

    match cli.command {
        Command::Plan { root, database_url } => cli::plan(&root, &database_url).await,
        Command::Apply { root, database_url, yes } => cli::apply(&root, &database_url, yes).await,
        Command::Import {
            root,
            database_url,
            dry_run,
            yes,
        } => cli::import(&root, &database_url, dry_run, yes).await,
        Command::Diff {
            database_url,
            root,
            copy_schema_objects,
        } => cli::diff(&database_url, &root, copy_schema_objects).await,
    }
}
