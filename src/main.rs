mod cli;
mod discovery;
mod executor;
mod hooks;
mod ledger;
mod lock;
mod migration_parser;
mod parser;
mod planner;
mod schema;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let extract_up = cli.extract_up;

    match cli.command {
        Command::Plan { root } => cli::plan(&root, extract_up).await,
        Command::Lock { root } => cli::lock(&root, extract_up).await,
        Command::Apply { root, database_url } => cli::apply(&root, &database_url, extract_up).await,
        Command::Status { database_url } => cli::status(&database_url).await,
        Command::Diff {
            database_url,
            root,
            copy_schema_objects,
        } => cli::diff(&database_url, &root, extract_up, copy_schema_objects).await,
        Command::Convert { source, output } => cli::convert(&source, &output).await,
    }
}
