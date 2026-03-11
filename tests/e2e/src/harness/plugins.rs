use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use once_cell::sync::OnceCell;

static PLUGIN_DIR: OnceCell<PathBuf> = OnceCell::new();

pub fn prepare_plugin_dir() -> Result<PathBuf> {
    PLUGIN_DIR
        .get_or_try_init(|| {
            let repo_root = repo_root();
            let output_dir = repo_root.join("target/plugins");
            std::fs::create_dir_all(&output_dir).context("failed to create target/plugins")?;

            ensure_plugin(
                &repo_root,
                "sources/postgres",
                "source_postgres",
                "postgres",
                &output_dir,
                "sources",
            )?;
            ensure_plugin(
                &repo_root,
                "destinations/postgres",
                "dest_postgres",
                "postgres",
                &output_dir,
                "destinations",
            )?;
            ensure_plugin(
                &repo_root,
                "transforms/sql",
                "transform_sql",
                "sql",
                &output_dir,
                "transforms",
            )?;
            ensure_plugin(
                &repo_root,
                "transforms/validate",
                "transform_validate",
                "validate",
                &output_dir,
                "transforms",
            )?;

            Ok(output_dir)
        })
        .cloned()
}

fn ensure_plugin(
    repo_root: &Path,
    plugin_subpath: &str,
    wasm_file_stem: &str,
    output_name: &str,
    output_dir: &Path,
    kind_subdir: &str,
) -> Result<()> {
    let kind_dir = output_dir.join(kind_subdir);
    std::fs::create_dir_all(&kind_dir)
        .with_context(|| format!("failed to create {}", kind_dir.display()))?;
    let target_file = kind_dir.join(format!("{output_name}.wasm"));

    let plugin_dir = repo_root.join("plugins").join(plugin_subpath);
    let status = Command::new("cargo")
        .arg("build")
        .current_dir(&plugin_dir)
        .status()
        .with_context(|| format!("failed to run cargo build in {}", plugin_dir.display()))?;

    if !status.success() {
        bail!("cargo build failed in {}", plugin_dir.display());
    }

    let built_wasm = plugin_dir
        .join("target/wasm32-wasip2/debug")
        .join(format!("{wasm_file_stem}.wasm"));
    std::fs::copy(&built_wasm, &target_file).with_context(|| {
        format!(
            "failed to copy wasm {} -> {}",
            built_wasm.display(),
            target_file.display()
        )
    })?;

    Ok(())
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("tests/e2e must be nested under repository root")
        .to_path_buf()
}
