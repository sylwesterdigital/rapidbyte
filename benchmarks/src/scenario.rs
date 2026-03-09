#![cfg_attr(not(test), allow(dead_code))]

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rapidbyte_types::wire::Feature;
use serde::{Deserialize, Serialize};

use crate::workload::WorkloadFamily;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkKind {
    Pipeline,
    Source,
    Destination,
    Transform,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScenarioConnectorRef {
    pub kind: String,
    pub plugin: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadProfile {
    pub family: WorkloadFamily,
    pub rows: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionProfile {
    pub iterations: u32,
    #[serde(default)]
    pub warmups: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScenarioManifest {
    pub id: String,
    pub name: String,
    pub suite: String,
    pub kind: BenchmarkKind,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub connectors: Vec<ScenarioConnectorRef>,
    #[serde(default)]
    pub requires: Vec<Feature>,
    pub workload: WorkloadProfile,
    pub execution: ExecutionProfile,
}

impl ScenarioManifest {
    pub fn from_path(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read scenario {}", path.display()))?;
        let parsed: Self = serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse scenario {}", path.display()))?;

        if parsed.id.trim().is_empty() {
            bail!("scenario {} has empty id", path.display());
        }
        if parsed.suite.trim().is_empty() {
            bail!("scenario {} has empty suite", path.display());
        }

        Ok(parsed)
    }
}

pub fn discover_scenarios(root: &Path) -> Result<Vec<ScenarioManifest>> {
    let mut paths = Vec::new();
    collect_yaml_files(root, &mut paths)?;
    paths.sort();

    let mut scenarios = Vec::with_capacity(paths.len());
    for path in paths {
        scenarios.push(ScenarioManifest::from_path(&path)?);
    }
    Ok(scenarios)
}

pub fn filter_scenarios<'a>(
    scenarios: &'a [ScenarioManifest],
    suite: Option<&str>,
    tags: &[&str],
) -> Vec<&'a ScenarioManifest> {
    scenarios
        .iter()
        .filter(|scenario| suite.is_none_or(|wanted| scenario.suite == wanted))
        .filter(|scenario| {
            tags.iter()
                .all(|tag| scenario.tags.iter().any(|entry| entry == tag))
        })
        .collect()
}

pub fn validate_scenario_capabilities(
    scenario: &ScenarioManifest,
    available: &[Feature],
) -> Result<()> {
    let missing: Vec<String> = scenario
        .requires
        .iter()
        .filter(|feature| !available.contains(feature))
        .map(feature_name)
        .collect();

    if missing.is_empty() {
        return Ok(());
    }

    bail!(
        "scenario {} requires unsupported feature(s): {}",
        scenario.id,
        missing.join(", ")
    );
}

fn feature_name(feature: &Feature) -> String {
    serde_json::to_string(feature)
        .unwrap_or_else(|_| "\"unknown\"".to_string())
        .trim_matches('"')
        .to_string()
}

fn collect_yaml_files(root: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    if !root.exists() {
        bail!("scenario directory does not exist: {}", root.display());
    }

    for entry in fs::read_dir(root)
        .with_context(|| format!("failed to read scenario directory {}", root.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_yaml_files(&path, paths)?;
            continue;
        }

        if matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("yaml" | "yml")
        ) {
            paths.push(path);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time drift")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rapidbyte-benchmarks-{label}-{nanos}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn discovers_and_parses_scenarios() {
        let root = temp_dir("discover");
        let scenarios_dir = root.join("benchmarks/scenarios/pr");
        fs::create_dir_all(&scenarios_dir).expect("create scenarios dir");
        let scenario_path = scenarios_dir.join("smoke.yaml");
        fs::write(
            &scenario_path,
            r#"
id: pr_smoke_pipeline
name: PR smoke pipeline
suite: pr
kind: pipeline
tags: [pr, synthetic]
connectors:
  - kind: source
    plugin: postgres
  - kind: destination
    plugin: postgres
workload:
  family: narrow_append
  rows: 1000
execution:
  iterations: 3
  warmups: 1
"#,
        )
        .expect("write scenario");

        let scenarios = discover_scenarios(&root.join("benchmarks/scenarios")).expect("discover");
        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].id, "pr_smoke_pipeline");
        assert_eq!(scenarios[0].suite, "pr");
        assert_eq!(scenarios[0].kind, BenchmarkKind::Pipeline);
    }

    #[test]
    fn invalid_scenario_returns_useful_error() {
        let root = temp_dir("invalid");
        let scenario_path = root.join("invalid.yaml");
        fs::write(
            &scenario_path,
            r#"
name: Missing id
suite: pr
kind: pipeline
workload:
  family: narrow_append
  rows: 1000
execution:
  iterations: 3
"#,
        )
        .expect("write invalid scenario");

        let err = ScenarioManifest::from_path(&scenario_path).expect_err("should fail");
        let message = err.to_string();
        assert!(message.contains("failed to parse scenario"));
        assert!(message.contains("invalid.yaml"));
    }
}
