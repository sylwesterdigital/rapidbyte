use anyhow::{bail, Result};

use crate::scenario::BenchmarkKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceAdapter {
    Postgres,
}

impl SourceAdapter {
    pub fn from_plugin(plugin: &str) -> Result<Self> {
        match plugin {
            "postgres" => Ok(Self::Postgres),
            other => bail!("unsupported source adapter plugin {other}"),
        }
    }

    pub fn supports_kind(self, kind: BenchmarkKind) -> bool {
        matches!(kind, BenchmarkKind::Pipeline | BenchmarkKind::Source)
    }
}
