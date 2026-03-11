use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(author, version, about = "Connector-agnostic benchmark runner")]
pub struct BenchCli {
    #[command(subcommand)]
    pub command: BenchCommand,
}

#[derive(Debug, Subcommand)]
pub enum BenchCommand {
    Run(RunArgs),
    Compare(CompareArgs),
    Summary(SummaryArgs),
}

#[derive(Debug, clap::Args)]
pub struct RunArgs {
    #[arg(long, default_value = "benchmarks/scenarios")]
    pub scenario_dir: PathBuf,

    #[arg(long)]
    pub suite: Option<String>,

    #[arg(long)]
    pub env_profile: Option<String>,

    #[arg(long)]
    pub hardware_class: Option<String>,

    #[arg(long)]
    pub rapidbyte_bin: Option<PathBuf>,

    #[arg(long = "scenario")]
    pub scenarios: Vec<String>,

    #[arg(long, default_value = "target/benchmarks/results.jsonl")]
    pub output: PathBuf,
}

#[derive(Debug, clap::Args)]
pub struct CompareArgs {
    pub baseline: PathBuf,
    pub candidate: PathBuf,

    #[arg(long, default_value_t = 3)]
    pub min_samples: usize,

    #[arg(long, default_value_t = 10.0)]
    pub throughput_drop_pct: f64,

    #[arg(long, default_value_t = 15.0)]
    pub latency_increase_pct: f64,
}

#[derive(Debug, clap::Args)]
pub struct SummaryArgs {
    pub artifact: PathBuf,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;

    use super::{BenchCli, BenchCommand};

    #[test]
    fn run_args_accept_env_profile_flag() {
        let cli = BenchCli::parse_from([
            "rapidbyte-benchmarks",
            "run",
            "--suite",
            "lab",
            "--scenario",
            "pg_dest_insert",
            "--env-profile",
            "local-dev-postgres",
        ]);

        match cli.command {
            BenchCommand::Run(args) => {
                assert_eq!(args.suite.as_deref(), Some("lab"));
                assert_eq!(args.env_profile.as_deref(), Some("local-dev-postgres"));
                assert_eq!(args.scenarios, vec!["pg_dest_insert"]);
                assert!(args.hardware_class.is_none());
                assert!(args.rapidbyte_bin.is_none());
            }
            BenchCommand::Compare(_) | BenchCommand::Summary(_) => panic!("expected run command"),
        }
    }

    #[test]
    fn compare_args_accept_threshold_flags() {
        let cli = BenchCli::parse_from([
            "rapidbyte-benchmarks",
            "compare",
            "baseline.jsonl",
            "candidate.jsonl",
            "--min-samples",
            "1",
            "--throughput-drop-pct",
            "7.5",
            "--latency-increase-pct",
            "9.5",
        ]);

        match cli.command {
            BenchCommand::Compare(args) => {
                assert_eq!(args.baseline, PathBuf::from("baseline.jsonl"));
                assert_eq!(args.candidate, PathBuf::from("candidate.jsonl"));
                assert_eq!(args.min_samples, 1);
                assert_eq!(args.throughput_drop_pct, 7.5);
                assert_eq!(args.latency_increase_pct, 9.5);
            }
            BenchCommand::Run(_) | BenchCommand::Summary(_) => panic!("expected compare command"),
        }
    }

    #[test]
    fn summary_args_accept_artifact_path() {
        let cli = BenchCli::parse_from([
            "rapidbyte-benchmarks",
            "summary",
            "target/benchmarks/lab/pg_dest_copy.jsonl",
        ]);

        match cli.command {
            BenchCommand::Summary(args) => {
                assert_eq!(
                    args.artifact,
                    PathBuf::from("target/benchmarks/lab/pg_dest_copy.jsonl")
                );
            }
            BenchCommand::Run(_) | BenchCommand::Compare(_) => panic!("expected summary command"),
        }
    }
}
