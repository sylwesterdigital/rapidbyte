use anyhow::{bail, Result};

use crate::scenario::BenchmarkKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformAdapter {
    Sql,
    Validate,
}

impl TransformAdapter {
    pub fn from_plugin(plugin: &str) -> Result<Self> {
        match plugin {
            "sql" => Ok(Self::Sql),
            "validate" => Ok(Self::Validate),
            other => bail!("unsupported transform adapter plugin {other}"),
        }
    }

    pub fn supports_kind(self, kind: BenchmarkKind) -> bool {
        matches!(kind, BenchmarkKind::Pipeline | BenchmarkKind::Transform)
    }
}
