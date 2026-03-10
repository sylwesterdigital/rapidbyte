mod adapters;
mod artifact;
mod cli;
mod environment;
mod metrics;
mod output;
mod pipeline;
mod runner;
mod scenario;
mod summary;
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
            let output_path = root.join(&args.output);
            let written = runner::emit_scenario_artifacts(
                &root,
                &scenarios,
                args.suite.as_deref(),
                &args.scenarios,
                args.env_profile.as_deref(),
                &output_path,
            )?;
            println!(
                "benchmark runner: wrote {} artifact(s) from {} discovered scenario(s) to {}",
                written,
                scenarios.len(),
                output_path.display()
            );
        }
        BenchCommand::Compare(args) => {
            println!(
                "benchmark compare skeleton: baseline={} candidate={}",
                args.baseline.display(),
                args.candidate.display()
            );
        }
        BenchCommand::Summary(args) => {
            let root = std::env::current_dir()?;
            let artifact_path = root.join(&args.artifact);
            let rendered = summary::load_and_render_summary(&artifact_path)?;
            println!("{rendered}");
        }
    }

    Ok(())
}
