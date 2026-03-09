mod adapters;
mod artifact;
mod cli;
mod scenario;
mod workload;

use anyhow::Result;
use clap::Parser;

use crate::cli::{BenchCli, BenchCommand};

fn main() -> Result<()> {
    let cli = BenchCli::parse();

    match cli.command {
        BenchCommand::Run(args) => {
            let root = std::env::current_dir()?;
            let scenarios = scenario::discover_scenarios(&root.join(&args.scenario_dir))?;
            println!(
                "benchmark runner skeleton: discovered {} scenario(s) in {}",
                scenarios.len(),
                root.join(args.scenario_dir).display()
            );
        }
        BenchCommand::Compare(args) => {
            println!(
                "benchmark compare skeleton: baseline={} candidate={}",
                args.baseline.display(),
                args.candidate.display()
            );
        }
    }

    Ok(())
}
