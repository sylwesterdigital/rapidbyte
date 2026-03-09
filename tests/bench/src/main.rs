use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use once_cell::sync::OnceCell;
use serde_json::{Map, Value as JsonValue};
use serde_yaml::Value as YamlValue;
use tempfile::NamedTempFile;
use testcontainers::clients;
use testcontainers::core::WaitFor;
use testcontainers::GenericImage;
use tokio_postgres::NoTls;

const MODES: [&str; 4] = ["insert", "copy", "transform", "transform_filter"];

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Profile {
    Small,
    Medium,
    Large,
}

impl Profile {
    fn default_rows(self) -> u32 {
        match self {
            Self::Small => 100_000,
            Self::Medium => 50_000,
            Self::Large => 10_000,
        }
    }

    fn seed_sql(self, root: &Path) -> PathBuf {
        let name = match self {
            Self::Small => "bench_seed_small.sql",
            Self::Medium => "bench_seed_medium.sql",
            Self::Large => "bench_seed_large.sql",
        };
        root.join("tests/bench/fixtures/sql").join(name)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
        }
    }
}

#[derive(Clone, Debug, Subcommand)]
enum BenchCommand {
    Run(RunArgs),
    Compare(CompareArgs),
}

#[derive(Clone, Debug, Parser)]
#[command(author, version, about = "Rust-native benchmark orchestrator")]
struct Cli {
    #[command(subcommand)]
    command: BenchCommand,
}

#[derive(Clone, Debug, Parser)]
struct RunArgs {
    plugin: Option<String>,
    rows: Option<u32>,

    #[arg(long, value_enum)]
    profile: Profile,

    #[arg(long)]
    iters: Option<u32>,

    #[arg(long, action = ArgAction::SetTrue)]
    debug: bool,

    #[arg(long, action = ArgAction::SetTrue, conflicts_with = "no_aot")]
    aot: bool,

    #[arg(long = "no-aot", action = ArgAction::SetTrue)]
    no_aot: bool,

    #[arg(long = "cpu-profile", action = ArgAction::SetTrue)]
    cpu_profile: bool,
}

#[derive(Clone, Debug, Parser)]
struct CompareArgs {
    ref1: String,
    ref2: String,

    #[arg(long)]
    plugin: Option<String>,

    #[arg(long)]
    rows: Option<u32>,

    #[arg(long, value_enum)]
    profile: Profile,

    #[arg(long)]
    iters: Option<u32>,

    #[arg(long, action = ArgAction::SetTrue, conflicts_with = "no_aot")]
    aot: bool,

    #[arg(long = "no-aot", action = ArgAction::SetTrue)]
    no_aot: bool,
}

#[derive(Debug, Clone)]
struct BenchContext {
    root: PathBuf,
    plugin_dir: PathBuf,
    results_file: PathBuf,
    bench_session_id: String,
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

struct BranchGuard {
    root: PathBuf,
    original_ref: String,
}

impl Drop for BranchGuard {
    fn drop(&mut self) {
        let _ = Command::new("git")
            .arg("-C")
            .arg(&self.root)
            .arg("checkout")
            .arg(&self.original_ref)
            .arg("--quiet")
            .status();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        BenchCommand::Run(args) => run_command_with_session(args, None).await,
        BenchCommand::Compare(args) => compare_command(args).await,
    }
}

async fn run_command_with_session(args: RunArgs, session_id: Option<String>) -> Result<()> {
    let root = repo_root()?;
    configure_build_env();

    let build_mode = if args.debug { "debug" } else { "release" };
    let aot = resolve_aot(args.aot, args.no_aot);
    let plugins = resolve_plugins(args.plugin.as_deref())?;

    println!();
    println!("═══════════════════════════════════════════════");
    println!("  Rapidbyte Benchmark");
    println!("  Plugins: {}", plugins.join(" "));
    println!("  Profile: {}", args.profile.as_str());
    println!("  Build: {build_mode} | AOT: {aot}");
    println!("═══════════════════════════════════════════════");
    println!();

    build_host(&root, build_mode)?;
    build_plugins(&root, build_mode)?;
    let plugin_dir = stage_plugins(&root, build_mode)?;
    report_wasm_sizes(&plugin_dir)?;

    let context = init_context(root.clone(), plugin_dir, session_id.unwrap_or_else(make_bench_session_id)).await?;
    let rows = args.rows.unwrap_or(args.profile.default_rows());
    let iters = args.iters.unwrap_or(3);

    for plugin in plugins {
        run_plugin(
            &context,
            &plugin,
            rows,
            args.profile,
            iters,
            build_mode,
            aot,
        )
        .await?;
    }

    if args.cpu_profile {
        run_cpu_profile(&context.root, rows)?;
    }

    println!();
    println!("═══════════════════════════════════════════════");
    println!("  Benchmark complete");
    println!("═══════════════════════════════════════════════");
    Ok(())
}

async fn compare_command(args: CompareArgs) -> Result<()> {
    let root = repo_root()?;
    ensure_clean_worktree(&root)?;
    let original_ref = git_current_ref(&root)?;
    let _guard = BranchGuard {
        root: root.clone(),
        original_ref,
    };

    let sha1 = git_resolve_ref(&root, &args.ref1)?;
    let sha2 = git_resolve_ref(&root, &args.ref2)?;
    if sha1 == sha2 {
        bail!("Both refs resolve to the same commit: {sha1}");
    }

    println!();
    println!("═══════════════════════════════════════════════");
    println!("  Rapidbyte Benchmark Comparison");
    println!("  Ref 1: {} ({sha1})", args.ref1);
    println!("  Ref 2: {} ({sha2})", args.ref2);
    println!("═══════════════════════════════════════════════");
    println!();

    let run_args = RunArgs {
        plugin: args.plugin,
        rows: args.rows,
        profile: args.profile,
        iters: args.iters,
        debug: false,
        aot: args.aot,
        no_aot: args.no_aot,
        cpu_profile: false,
    };

    checkout_ref(&root, &sha1)?;
    let compare_session_id = make_bench_session_id();

    run_command_with_session(run_args.clone(), Some(compare_session_id.clone())).await?;

    checkout_ref(&root, &sha2)?;
    run_command_with_session(run_args, Some(compare_session_id.clone())).await?;

    let status = Command::new("python3")
        .arg(root.join("tests/bench/analyze.py"))
        .arg("--session-id")
        .arg(&compare_session_id)
        .arg("--sha")
        .arg(sha1)
        .arg(sha2)
        .arg("--profile")
        .arg(args.profile.as_str())
        .status()
        .context("failed to run tests/bench/analyze.py")?;
    if !status.success() {
        bail!("tests/bench/analyze.py failed");
    }

    Ok(())
}

fn resolve_aot(enable: bool, disable: bool) -> bool {
    if disable {
        return false;
    }
    if enable {
        return true;
    }
    true
}

fn configure_build_env() {
    if std::env::var("RAPIDBYTE_BENCH_DETERMINISTIC").as_deref() == Ok("1") {
        std::env::set_var("RUSTC_WRAPPER", "");
        std::env::set_var("CARGO_BUILD_RUSTC_WRAPPER", "");
        std::env::set_var("RUSTFLAGS", "");
        std::env::set_var("CARGO_TARGET_WASM32_WASIP2_RUSTFLAGS", "");
    }
}

fn resolve_plugins(selected: Option<&str>) -> Result<Vec<String>> {
    match selected {
        Some("postgres") | None => Ok(vec!["postgres".to_string()]),
        Some(other) => bail!("Unknown plugin: {other} (supported: postgres)"),
    }
}

fn repo_root() -> Result<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .context("tests/bench must be nested under repository root")?;
    Ok(root.to_path_buf())
}

fn build_host(root: &Path, mode: &str) -> Result<()> {
    println!("Building host ({mode})...");
    let mut cmd = Command::new("cargo");
    cmd.arg("build").arg("--quiet").current_dir(root);
    if mode == "release" {
        cmd.arg("--release");
    }
    ensure_success(
        cmd.status().context("failed to build host")?,
        "host build failed",
    )
}

fn build_plugins(root: &Path, mode: &str) -> Result<()> {
    let subpaths = [
        "sources/postgres",
        "destinations/postgres",
        "transforms/sql",
    ];
    for subpath in subpaths {
        println!("Building {subpath} plugin ({mode})...");
        let mut cmd = Command::new("cargo");
        cmd.arg("build")
            .arg("--quiet")
            .current_dir(root.join("plugins").join(subpath));
        if mode == "release" {
            cmd.arg("--release");
        }
        ensure_success(
            cmd.status()
                .with_context(|| format!("failed to build plugin {subpath}"))?,
            &format!("plugin build failed for {subpath}"),
        )?;
    }
    Ok(())
}

fn stage_plugins(root: &Path, mode: &str) -> Result<PathBuf> {
    let output_dir = root.join("target/plugins");
    fs::create_dir_all(&output_dir).context("failed to create target/plugins")?;

    let wasm_dir = format!("wasm32-wasip2/{mode}");
    let mappings = [
        (
            "sources/postgres",
            "source_postgres.wasm",
            "postgres.wasm",
            "sources",
            "source_postgres.wasm",
        ),
        (
            "destinations/postgres",
            "dest_postgres.wasm",
            "postgres.wasm",
            "destinations",
            "dest_postgres.wasm",
        ),
        (
            "transforms/sql",
            "transform_sql.wasm",
            "sql.wasm",
            "transforms",
            "transform_sql.wasm",
        ),
    ];

    for (subpath, file_name, output_name, kind_subdir, legacy_name) in mappings {
        let kind_dir = output_dir.join(kind_subdir);
        fs::create_dir_all(&kind_dir)
            .with_context(|| format!("failed to create {}", kind_dir.display()))?;

        let src = root
            .join("plugins")
            .join(subpath)
            .join("target")
            .join(&wasm_dir)
            .join(file_name);
        let dest = kind_dir.join(output_name);
        fs::copy(&src, &dest).with_context(|| {
            format!(
                "failed to stage plugin wasm {} -> {}",
                src.display(),
                dest.display()
            )
        })?;
        let legacy_dest = output_dir.join(legacy_name);
        fs::copy(&src, &legacy_dest).with_context(|| {
            format!(
                "failed to stage legacy connector wasm {} -> {}",
                src.display(),
                legacy_dest.display()
            )
        })?;

        if mode == "release" {
            let strip = root.join("scripts/strip-wasm.sh");
            if strip.exists() {
                ensure_success(
                    Command::new(&strip)
                        .arg(&dest)
                        .arg(&dest)
                        .status()
                        .with_context(|| format!("failed to strip {}", dest.display()))?,
                    "wasm strip failed",
                )?;
                ensure_success(
                    Command::new(&strip)
                        .arg(&legacy_dest)
                        .arg(&legacy_dest)
                        .status()
                        .with_context(|| format!("failed to strip {}", legacy_dest.display()))?,
                    "wasm strip failed",
                )?;
            }
        }
    }

    Ok(output_dir)
}

fn report_wasm_sizes(plugin_dir: &Path) -> Result<()> {
    println!("Plugin binary sizes:");
    for kind in ["sources", "destinations", "transforms"] {
        let kind_dir = plugin_dir.join(kind);
        if !kind_dir.exists() {
            continue;
        }
        for entry in fs::read_dir(&kind_dir)
            .with_context(|| format!("failed to list staged plugins in {}", kind_dir.display()))?
        {
            let path = entry?.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("wasm") {
                continue;
            }
            let size = fs::metadata(&path)?.len();
            let human = if size >= 1_048_576 {
                format!("{:.1} MB", size as f64 / 1_048_576.0)
            } else {
                format!("{:.0} KB", size as f64 / 1024.0)
            };
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("unknown");
            println!("  {kind}/{name}: {human} ({size} bytes)");
        }
    }
    Ok(())
}

async fn init_context(
    root: PathBuf,
    plugin_dir: PathBuf,
    bench_session_id: String,
) -> Result<BenchContext> {
    let port = shared_postgres_port()?;
    let results_dir = root.join("target/bench_results");
    fs::create_dir_all(&results_dir).context("failed to create target/bench_results")?;

    Ok(BenchContext {
        root,
        plugin_dir,
        results_file: results_dir.join("results.jsonl"),
        bench_session_id,
        host: "127.0.0.1".to_string(),
        port,
        user: "postgres".to_string(),
        password: "postgres".to_string(),
        database: "rapidbyte_test".to_string(),
    })
}

async fn run_plugin(
    context: &BenchContext,
    plugin: &str,
    rows: u32,
    profile: Profile,
    iters: u32,
    build_mode: &str,
    aot: bool,
) -> Result<()> {
    if plugin != "postgres" {
        bail!("unsupported plugin {plugin}");
    }

    println!();
    println!(
        "Setting up {plugin} benchmark ({rows} rows, profile: {})",
        profile.as_str()
    );
    seed_source_table(context, profile, rows).await?;

    let mut mode_results: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();

    for mode in MODES {
        println!("Running {iters} iterations ({mode} mode, {rows} rows)...");
        for i in 1..=iters {
            clean_dest_schema(context).await?;
            remove_state_db()?;

            let pipeline_path = render_pipeline(context, mode)?;
            let started = Instant::now();
            let json_line = run_pipeline_once(context, &pipeline_path, build_mode, aot)?;
            let elapsed = started.elapsed().as_secs_f64();

            let mut json = serde_json::from_str::<JsonValue>(&json_line)
                .context("failed to parse benchmark JSON output")?;
            json["wall_duration_secs"] = JsonValue::from(elapsed);

            let duration = json
                .get("duration_secs")
                .and_then(JsonValue::as_f64)
                .unwrap_or(0.0);
            println!("  [{mode}] Iteration {i}/{iters} ... done ({duration:.3}s)");

            let enriched = enrich_result(json, mode, rows, aot, profile, context)?;
            append_result(&context.results_file, &enriched)?;
            mode_results
                .entry(mode)
                .or_default()
                .push(serde_json::to_string(&enriched)?);
        }
        println!();
    }

    run_report(context, rows, profile, &mode_results)?;
    Ok(())
}

fn run_pipeline_once(
    context: &BenchContext,
    pipeline_path: &Path,
    build_mode: &str,
    aot: bool,
) -> Result<String> {
    let binary = context
        .root
        .join("target")
        .join(build_mode)
        .join("rapidbyte");
    let output = Command::new(&binary)
        .arg("run")
        .arg(pipeline_path)
        .arg("--log-level")
        .arg("warn")
        .env("RAPIDBYTE_PLUGIN_DIR", &context.plugin_dir)
        .env("RAPIDBYTE_CONNECTOR_DIR", &context.plugin_dir)
        .env("RAPIDBYTE_WASMTIME_AOT", if aot { "1" } else { "0" })
        .env("RAPIDBYTE_BENCH", "1")
        .output()
        .with_context(|| format!("failed to run pipeline using {}", binary.display()))?;

    if !output.status.success() {
        bail!(
            "pipeline failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let marker = stdout
        .lines()
        .find_map(|line| line.strip_prefix("@@BENCH_JSON@@"))
        .ok_or_else(|| anyhow!("pipeline succeeded but did not emit @@BENCH_JSON@@ marker"))?;
    Ok(marker.to_string())
}

fn append_result(path: &Path, json: &JsonValue) -> Result<()> {
    let line = serde_json::to_string(json)?;
    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    writeln!(file, "{line}").context("failed to append benchmark result")?;
    Ok(())
}

fn run_report(
    context: &BenchContext,
    rows: u32,
    profile: Profile,
    mode_results: &BTreeMap<&str, Vec<String>>,
) -> Result<()> {
    println!("Benchmark Report");

    let mut temp_files = Vec::new();
    let mut cmd = Command::new("python3");
    cmd.arg(context.root.join("tests/bench/lib/report.py"));
    cmd.arg(rows.to_string());
    cmd.arg(profile.as_str());

    for (mode, lines) in mode_results {
        if lines.is_empty() {
            continue;
        }
        let file = NamedTempFile::new().context("failed to create report temp file")?;
        fs::write(file.path(), lines.join("\n") + "\n").context("failed to write report temp")?;
        cmd.arg(format!("{mode}:{}", file.path().display()));
        temp_files.push(file);
    }

    ensure_success(
        cmd.status().context("failed to run report.py")?,
        "benchmark report failed",
    )?;

    drop(temp_files);
    Ok(())
}

fn enrich_result(
    mut json: JsonValue,
    mode: &str,
    rows: u32,
    aot: bool,
    profile: Profile,
    context: &BenchContext,
) -> Result<JsonValue> {
    let obj = json
        .as_object_mut()
        .ok_or_else(|| anyhow!("benchmark JSON must be an object"))?;

    obj.insert("timestamp".to_string(), JsonValue::String(now_iso8601()));
    obj.insert("mode".to_string(), JsonValue::String(mode.to_string()));
    obj.insert("bench_rows".to_string(), JsonValue::from(rows));
    obj.insert("aot".to_string(), JsonValue::from(aot));
    obj.insert(
        "git_sha".to_string(),
        JsonValue::String(git_sha(&context.root)?),
    );
    obj.insert(
        "git_branch".to_string(),
        JsonValue::String(git_branch(&context.root)?),
    );
    obj.insert(
        "profile".to_string(),
        JsonValue::String(profile.as_str().to_string()),
    );
    obj.insert(
        "bench_session_id".to_string(),
        JsonValue::String(context.bench_session_id.clone()),
    );

    Ok(JsonValue::Object(Map::from_iter(obj.clone())))
}

fn make_bench_session_id() -> String {
    let epoch_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("bench-{}-{}", std::process::id(), epoch_nanos)
}

fn now_iso8601() -> String {
    let output = Command::new("date")
        .arg("-u")
        .arg("+%Y-%m-%dT%H:%M:%SZ")
        .output();

    match output {
        Ok(result) if result.status.success() => {
            String::from_utf8_lossy(&result.stdout).trim().to_string()
        }
        _ => "1970-01-01T00:00:00Z".to_string(),
    }
}

async fn seed_source_table(context: &BenchContext, profile: Profile, rows: u32) -> Result<()> {
    let client = connect_postgres(context).await?;
    client
        .batch_execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        .await
        .context("failed to ensure pgcrypto extension")?;

    let seed_path = profile.seed_sql(&context.root);
    let sql = fs::read_to_string(&seed_path)
        .with_context(|| format!("failed to read seed SQL fixture {}", seed_path.display()))?;
    let sql = sql
        .replace(":'bench_rows'", &rows.to_string())
        .replace(":bench_rows", &rows.to_string());

    client
        .batch_execute(&sql)
        .await
        .context("failed to seed benchmark source data")?;

    let count = client
        .query_one("SELECT COUNT(*) FROM public.bench_events", &[])
        .await
        .context("failed to verify seeded row count")?
        .get::<_, i64>(0);
    if count != i64::from(rows) {
        bail!("expected {rows} seeded rows, got {count}");
    }
    Ok(())
}

async fn clean_dest_schema(context: &BenchContext) -> Result<()> {
    let client = connect_postgres(context).await?;
    client
        .batch_execute("DROP SCHEMA IF EXISTS raw CASCADE")
        .await
        .context("failed to clean destination schema")?;
    Ok(())
}

fn remove_state_db() -> Result<()> {
    let path = Path::new("/tmp/rapidbyte_bench_state.db");
    if path.exists() {
        fs::remove_file(path).context("failed to remove /tmp/rapidbyte_bench_state.db")?;
    }
    Ok(())
}

fn render_pipeline(context: &BenchContext, mode: &str) -> Result<PathBuf> {
    let file = match mode {
        "insert" => "bench_pg.yaml",
        "copy" => "bench_pg_copy.yaml",
        "transform" => "bench_pg_transform.yaml",
        "transform_filter" => "bench_pg_transform_filter.yaml",
        _ => bail!("unknown benchmark mode {mode}"),
    };

    let template_path = context.root.join("tests/bench/fixtures/pipelines").join(file);
    let template = fs::read_to_string(&template_path)
        .with_context(|| format!("failed to read {}", template_path.display()))?;

    let mut yaml: YamlValue = serde_yaml::from_str(&template).context("failed to parse YAML")?;
    set_pipeline_connection(&mut yaml, "source", context)?;
    set_pipeline_connection(&mut yaml, "destination", context)?;

    if let Some(state) = yaml.get_mut("state").and_then(YamlValue::as_mapping_mut) {
        state.insert(
            YamlValue::String("connection".to_string()),
            YamlValue::String("/tmp/rapidbyte_bench_state.db".to_string()),
        );
    }

    let rendered = serde_yaml::to_string(&yaml).context("failed to render benchmark YAML")?;
    let temp = NamedTempFile::new().context("failed to create temp pipeline file")?;
    fs::write(temp.path(), rendered).context("failed to write temp benchmark pipeline")?;
    let (_, path) = temp.keep().context("failed to persist temp pipeline")?;
    Ok(path)
}

fn set_pipeline_connection(yaml: &mut YamlValue, key: &str, context: &BenchContext) -> Result<()> {
    let section = yaml
        .get_mut(key)
        .and_then(YamlValue::as_mapping_mut)
        .ok_or_else(|| anyhow!("missing {key} section"))?;
    let config = section
        .get_mut(YamlValue::String("config".to_string()))
        .and_then(YamlValue::as_mapping_mut)
        .ok_or_else(|| anyhow!("missing {key}.config section"))?;

    config.insert(
        YamlValue::String("host".to_string()),
        YamlValue::String(context.host.clone()),
    );
    config.insert(
        YamlValue::String("port".to_string()),
        YamlValue::Number(serde_yaml::Number::from(context.port)),
    );
    config.insert(
        YamlValue::String("user".to_string()),
        YamlValue::String(context.user.clone()),
    );
    config.insert(
        YamlValue::String("password".to_string()),
        YamlValue::String(context.password.clone()),
    );
    config.insert(
        YamlValue::String("database".to_string()),
        YamlValue::String(context.database.clone()),
    );

    Ok(())
}

async fn connect_postgres(context: &BenchContext) -> Result<tokio_postgres::Client> {
    let conn = format!(
        "host={} port={} user={} password={} dbname={}",
        context.host, context.port, context.user, context.password, context.database
    );
    let (client, connection) = tokio_postgres::connect(&conn, NoTls)
        .await
        .context("failed to connect to benchmark postgres")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

fn run_cpu_profile(root: &Path, rows: u32) -> Result<()> {
    let samply = Command::new("samply").arg("--version").status();
    if !matches!(samply, Ok(status) if status.success()) {
        bail!("samply not found. Install with: cargo install samply");
    }

    println!("Running profiling iteration with samply...");
    let profile_dir = root.join("target/profiles");
    fs::create_dir_all(&profile_dir).context("failed to create target/profiles")?;

    let timestamp = now_compact_timestamp();
    let profile_file = profile_dir.join(format!("profile_{rows}rows_insert_{timestamp}.json"));

    ensure_success(
        Command::new("samply")
            .arg("record")
            .arg("--save-only")
            .arg("-o")
            .arg(&profile_file)
            .arg("--")
            .arg(root.join("target/release/rapidbyte"))
            .arg("run")
            .arg(root.join("tests/bench/fixtures/pipelines/bench_pg.yaml"))
            .arg("--log-level")
            .arg("warn")
            .env("RAPIDBYTE_PLUGIN_DIR", root.join("target/plugins"))
            .env("RAPIDBYTE_CONNECTOR_DIR", root.join("target/plugins"))
            .status()
            .context("failed to run samply record")?,
        "profiling run failed",
    )?;

    println!("Profile saved to: {}", profile_file.display());
    Ok(())
}

fn now_compact_timestamp() -> String {
    let output = Command::new("date").arg("+%Y%m%d_%H%M%S").output();
    match output {
        Ok(result) if result.status.success() => {
            String::from_utf8_lossy(&result.stdout).trim().to_string()
        }
        _ => "19700101_000000".to_string(),
    }
}

fn ensure_clean_worktree(root: &Path) -> Result<()> {
    let status1 = Command::new("git")
        .arg("-C")
        .arg(root)
        .arg("diff")
        .arg("--quiet")
        .status()
        .context("failed to inspect git working tree")?;
    let status2 = Command::new("git")
        .arg("-C")
        .arg(root)
        .arg("diff")
        .arg("--cached")
        .arg("--quiet")
        .status()
        .context("failed to inspect git index")?;

    if !status1.success() || !status2.success() {
        bail!("Working tree is dirty. Commit or stash changes before comparing.");
    }
    Ok(())
}

fn git_current_ref(root: &Path) -> Result<String> {
    let symbolic = Command::new("git")
        .arg("-C")
        .arg(root)
        .arg("symbolic-ref")
        .arg("--short")
        .arg("HEAD")
        .output()
        .context("failed to resolve current branch")?;
    if symbolic.status.success() {
        return Ok(String::from_utf8_lossy(&symbolic.stdout).trim().to_string());
    }

    let detached = Command::new("git")
        .arg("-C")
        .arg(root)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .context("failed to resolve detached HEAD")?;
    if !detached.status.success() {
        bail!("failed to determine current git ref");
    }
    Ok(String::from_utf8_lossy(&detached.stdout).trim().to_string())
}

fn git_resolve_ref(root: &Path, reference: &str) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .arg("rev-parse")
        .arg("--short")
        .arg(reference)
        .output()
        .with_context(|| format!("failed to resolve git ref {reference}"))?;
    if !output.status.success() {
        bail!("Invalid ref: {reference}");
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_sha(root: &Path) -> Result<String> {
    git_resolve_ref(root, "HEAD")
}

fn git_branch(root: &Path) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .arg("branch")
        .arg("--show-current")
        .output()
        .context("failed to resolve git branch")?;
    if !output.status.success() {
        return Ok("unknown".to_string());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn checkout_ref(root: &Path, reference: &str) -> Result<()> {
    ensure_success(
        Command::new("git")
            .arg("-C")
            .arg(root)
            .arg("checkout")
            .arg(reference)
            .arg("--quiet")
            .status()
            .with_context(|| format!("failed to checkout ref {reference}"))?,
        &format!("checkout failed for {reference}"),
    )
}

fn ensure_success(status: std::process::ExitStatus, message: &str) -> Result<()> {
    if status.success() {
        Ok(())
    } else {
        bail!(message.to_string())
    }
}

static SHARED_PORT: OnceCell<u16> = OnceCell::new();

fn shared_postgres_port() -> Result<u16> {
    SHARED_PORT
        .get_or_try_init(|| {
            let docker: &'static clients::Cli = Box::leak(Box::new(clients::Cli::default()));
            let image = GenericImage::new("postgres", "16-alpine")
                .with_env_var("POSTGRES_USER", "postgres")
                .with_env_var("POSTGRES_PASSWORD", "postgres")
                .with_env_var("POSTGRES_DB", "rapidbyte_test")
                .with_exposed_port(5432)
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ));
            let args = vec![
                "-c".to_string(),
                "wal_level=logical".to_string(),
                "-c".to_string(),
                "max_replication_slots=16".to_string(),
                "-c".to_string(),
                "max_wal_senders=16".to_string(),
            ];
            let node = Box::leak(Box::new(docker.run((image, args))));
            let port = node.get_host_port_ipv4(5432);
            Ok(port)
        })
        .copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_bench_context() -> BenchContext {
        BenchContext {
            root: repo_root().expect("repo root"),
            plugin_dir: PathBuf::from("/tmp/rapidbyte-test-plugins"),
            results_file: PathBuf::from("/tmp/rapidbyte-test-results.jsonl"),
            bench_session_id: "test-session".to_string(),
            host: "127.0.0.1".to_string(),
            port: 5433,
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            database: "rapidbyte_test".to_string(),
        }
    }

    #[test]
    fn profile_defaults_match_expected_rows() {
        assert_eq!(Profile::Small.default_rows(), 100_000);
        assert_eq!(Profile::Medium.default_rows(), 50_000);
        assert_eq!(Profile::Large.default_rows(), 10_000);
    }

    #[test]
    fn aot_defaults_to_enabled() {
        assert!(resolve_aot(false, false));
        assert!(resolve_aot(true, false));
        assert!(!resolve_aot(false, true));
    }

    #[test]
    fn insert_benchmark_fixture_explicitly_uses_insert() {
        let pipeline_path = render_pipeline(&test_bench_context(), "insert").expect("render insert");
        let rendered = fs::read_to_string(&pipeline_path).expect("read rendered pipeline");
        let yaml: YamlValue = serde_yaml::from_str(&rendered).expect("parse rendered pipeline");

        let load_method = yaml
            .get("destination")
            .and_then(YamlValue::as_mapping)
            .and_then(|destination| destination.get(YamlValue::String("config".to_string())))
            .and_then(YamlValue::as_mapping)
            .and_then(|config| config.get(YamlValue::String("load_method".to_string())))
            .and_then(YamlValue::as_str);

        assert_eq!(load_method, Some("insert"));
    }
}
