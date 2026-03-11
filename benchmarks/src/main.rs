mod adapters;
mod artifact;
mod cli;
mod compare;
mod environment;
mod isolation;
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
                runner::EmitArtifactsOptions {
                    suite: args.suite.as_deref(),
                    scenario_ids: &args.scenarios,
                    env_profile: args.env_profile.as_deref(),
                    hardware_class: args.hardware_class.as_deref(),
                    rapidbyte_bin: args.rapidbyte_bin.as_deref(),
                    output_path: &output_path,
                },
            )?;
            println!(
                "benchmark runner: wrote {} artifact(s) from {} discovered scenario(s) to {}",
                written,
                scenarios.len(),
                output_path.display()
            );
        }
        BenchCommand::Compare(args) => {
            let rendered = compare::load_and_render_comparison(
                &args.baseline,
                &args.candidate,
                args.min_samples,
                args.throughput_drop_pct,
                args.latency_increase_pct,
            )?;
            println!("{rendered}");
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
