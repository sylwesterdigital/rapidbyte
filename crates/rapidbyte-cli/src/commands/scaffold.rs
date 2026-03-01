//! Connector project scaffolding subcommand (scaffold).

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

/// Connector role derived from the name prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Source,
    Destination,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Role::Source => "source",
            Role::Destination => "destination",
        }
    }
}

/// Run the scaffold command.
///
/// # Errors
///
/// Returns `Err` if the connector name is invalid, the output directory
/// cannot be created, or file writing fails.
#[allow(clippy::too_many_lines)] // Scaffolding requires writing many files with sequential steps.
pub fn run(name: &str, output: Option<&str>) -> Result<()> {
    // Determine role from name prefix
    let role = if name.starts_with("source-") {
        Role::Source
    } else if name.starts_with("dest-") {
        Role::Destination
    } else {
        bail!(
            "Connector name must start with 'source-' or 'dest-', got '{name}'"
        );
    };

    // Compute output directory
    let base_dir = match output {
        Some(dir) => PathBuf::from(dir).join(name),
        None => PathBuf::from("connectors").join(name),
    };

    if base_dir.exists() {
        bail!("Directory already exists: {}", base_dir.display());
    }

    // Derive names
    let name_underscored = name.replace('-', "_");
    let struct_name = to_struct_name(name);
    let service_name = extract_service_name(name, role);
    let display_name = format!(
        "Rapidbyte {} {}",
        if role == Role::Source {
            "Source"
        } else {
            "Destination"
        },
        service_name
    );

    // Create directories
    let src_dir = base_dir.join("src");
    let cargo_dir = base_dir.join(".cargo");
    fs::create_dir_all(&src_dir).context("Failed to create src directory")?;
    fs::create_dir_all(&cargo_dir).context("Failed to create .cargo directory")?;

    // Track created files for summary
    let mut created_files: Vec<PathBuf> = Vec::new();

    // Write Cargo.toml
    let cargo_toml = gen_cargo_toml(name, &name_underscored);
    write_file(
        &base_dir.join("Cargo.toml"),
        &cargo_toml,
        &mut created_files,
    )?;

    // Write .cargo/config.toml
    let cargo_config = gen_cargo_config();
    write_file(
        &cargo_dir.join("config.toml"),
        &cargo_config,
        &mut created_files,
    )?;

    // Write build.rs
    let build_rs = gen_build_rs(name, &display_name, role, &service_name);
    write_file(&base_dir.join("build.rs"), &build_rs, &mut created_files)?;

    // Write source files
    match role {
        Role::Source => {
            write_file(
                &src_dir.join("main.rs"),
                &gen_source_main(&struct_name),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("config.rs"),
                &gen_config(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("client.rs"),
                &gen_client_rs(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("reader.rs"),
                &gen_reader_rs(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("schema.rs"),
                &gen_schema_rs(),
                &mut created_files,
            )?;
        }
        Role::Destination => {
            write_file(
                &src_dir.join("main.rs"),
                &gen_dest_main(&struct_name),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("config.rs"),
                &gen_config(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("client.rs"),
                &gen_client_rs(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("writer.rs"),
                &gen_writer_rs(),
                &mut created_files,
            )?;
        }
    }

    // Print summary
    println!("Scaffolded {} connector '{}'", role.as_str(), name);
    println!();
    println!("Created files:");
    for f in &created_files {
        println!("  {}", f.display());
    }
    println!();
    println!("Next steps:");
    println!("  1. cd {}", base_dir.display());
    println!("  2. Edit src/config.rs with your connection parameters");
    match role {
        Role::Source => {
            println!("  3. Implement connection validation in src/client.rs");
            println!("  4. Implement schema discovery in src/schema.rs");
            println!("  5. Implement stream reading in src/reader.rs");
        }
        Role::Destination => {
            println!("  3. Implement connection validation in src/client.rs");
            println!("  4. Implement stream writing in src/writer.rs");
        }
    }
    println!("  Then: cargo build --release");

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn write_file(path: &Path, content: &str, created: &mut Vec<PathBuf>) -> Result<()> {
    fs::write(path, content).with_context(|| format!("Failed to write {}", path.display()))?;
    created.push(path.to_path_buf());
    Ok(())
}

/// Convert `source-mysql` -> `SourceMysql`, `dest-snowflake` -> `DestSnowflake`.
fn to_struct_name(name: &str) -> String {
    name.split('-')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().to_string() + chars.as_str(),
            }
        })
        .collect()
}

/// Extract the service name: `source-mysql` -> `MySQL`, `dest-snowflake` -> `Snowflake`.
/// Falls back to title-casing the suffix.
fn extract_service_name(name: &str, role: Role) -> String {
    let prefix = match role {
        Role::Source => "source-",
        Role::Destination => "dest-",
    };
    let suffix = name.strip_prefix(prefix).unwrap_or(name);

    // Title-case: first char uppercase, rest lowercase
    let mut chars = suffix.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_uppercase().to_string() + chars.as_str(),
    }
}

// ---------------------------------------------------------------------------
// Template generators
// ---------------------------------------------------------------------------

fn gen_cargo_toml(name: &str, name_underscored: &str) -> String {
    format!(
        r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2021"

[workspace]

[[bin]]
name = "{name_underscored}"
path = "src/main.rs"

[dependencies]
rapidbyte-sdk = {{ path = "../../crates/rapidbyte-sdk" }}
serde = {{ version = "1", features = ["derive"] }}
serde_json = "1"
tokio = {{ version = "1.36", features = ["rt", "macros", "io-util"] }}
arrow = {{ version = "53", features = ["ipc"] }}

[build-dependencies]
rapidbyte-sdk = {{ path = "../../crates/rapidbyte-sdk", default-features = false, features = ["build"] }}
"#
    )
}

fn gen_cargo_config() -> String {
    r#"[build]
target = "wasm32-wasip2"
"#
    .to_string()
}

fn gen_build_rs(name: &str, display_name: &str, role: Role, service_name: &str) -> String {
    match role {
        Role::Source => format!(
            r#"use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::SyncMode;

fn main() {{
    ManifestBuilder::source("rapidbyte/{name}")
        .name("{display_name}")
        .description("Source connector for {service_name}")
        .sync_modes(&[SyncMode::FullRefresh])
        .allow_runtime_network()
        .emit();
}}
"#
        ),
        Role::Destination => format!(
            r#"use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::WriteMode;

fn main() {{
    ManifestBuilder::destination("rapidbyte/{name}")
        .name("{display_name}")
        .description("Destination connector for {service_name}")
        .write_modes(&[WriteMode::Append])
        .allow_runtime_network()
        .emit();
}}
"#
        ),
    }
}

fn gen_source_main(struct_name: &str) -> String {
    format!(
        r#"pub mod config;
mod client;
mod reader;
pub mod schema;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::connector(source)]
pub struct {struct_name} {{
    config: config::Config,
}}

impl Source for {struct_name} {{
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {{
        Ok((
            Self {{ config }},
            ConnectorInfo {{
                protocol_version: ProtocolVersion::V4,
                features: vec![],
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            }},
        ))
    }}

    async fn discover(&mut self, _ctx: &Context) -> Result<Catalog, ConnectorError> {{
        schema::discover_catalog(&self.config)
    }}

    async fn validate(config: &Self::Config, _ctx: &Context) -> Result<ValidationResult, ConnectorError> {{
        client::validate(config).await
    }}

    async fn read(&mut self, ctx: &Context, stream: StreamContext) -> Result<ReadSummary, ConnectorError> {{
        reader::read_stream(&self.config, ctx, &stream).await
    }}

    async fn close(&mut self, ctx: &Context) -> Result<(), ConnectorError> {{
        ctx.log(LogLevel::Info, &format!("{{}}: close", env!("CARGO_PKG_NAME")));
        Ok(())
    }}
}}
"#
    )
}

fn gen_dest_main(struct_name: &str) -> String {
    format!(
        r#"mod config;
mod client;
mod writer;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::connector(destination)]
pub struct {struct_name} {{
    config: config::Config,
}}

impl Destination for {struct_name} {{
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, ConnectorInfo), ConnectorError> {{
        Ok((
            Self {{ config }},
            ConnectorInfo {{
                protocol_version: ProtocolVersion::V4,
                features: vec![],
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            }},
        ))
    }}

    async fn validate(config: &Self::Config, _ctx: &Context) -> Result<ValidationResult, ConnectorError> {{
        client::validate(config).await
    }}

    async fn write(&mut self, ctx: &Context, stream: StreamContext) -> Result<WriteSummary, ConnectorError> {{
        writer::write_stream(&self.config, ctx, &stream).await
    }}

    async fn close(&mut self, ctx: &Context) -> Result<(), ConnectorError> {{
        ctx.log(LogLevel::Info, &format!("{{}}: close", env!("CARGO_PKG_NAME")));
        Ok(())
    }}
}}
"#
    )
}

fn gen_config() -> String {
    r#"use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

/// Connection config deserialized from pipeline YAML.
#[derive(Debug, Clone, Deserialize, ConfigSchema)]
pub struct Config {
    /// Hostname
    pub host: String,
    /// Port
    #[serde(default = "default_port")]
    #[schema(default = 3306)]
    pub port: u16,
    /// Database user
    pub user: String,
    /// Database password
    #[serde(default)]
    #[schema(secret)]
    pub password: String,
    /// Database name
    pub database: String,
}

fn default_port() -> u16 {
    3306 // TODO: Change to your connector's default port
}

impl Config {
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.database
        )
    }
}
"#
    .to_string()
}

fn gen_client_rs() -> String {
    r#"use rapidbyte_sdk::prelude::*;
use crate::config::Config;

pub async fn validate(config: &Config) -> Result<ValidationResult, ConnectorError> {
    // TODO: Connect and run a test query
    let _ = config;
    Ok(ValidationResult {
        status: ValidationStatus::Success,
        message: "Validation not yet implemented".to_string(),
    })
}
"#
    .to_string()
}

fn gen_reader_rs() -> String {
    r"use rapidbyte_sdk::prelude::*;
use crate::config::Config;

pub async fn read_stream(config: &Config, ctx: &Context, stream: &StreamContext) -> Result<ReadSummary, ConnectorError> {
    // TODO: Implement stream reading
    let _ = (config, ctx, stream);
    Ok(ReadSummary {
        records_read: 0,
        bytes_read: 0,
        batches_emitted: 0,
        checkpoint_count: 0,
        records_skipped: 0,
        perf: None,
    })
}
"
    .to_string()
}

fn gen_schema_rs() -> String {
    r"use rapidbyte_sdk::prelude::*;
use crate::config::Config;

pub fn discover_catalog(config: &Config) -> Result<Catalog, ConnectorError> {
    // TODO: Implement schema discovery
    let _ = config;
    Ok(Catalog { streams: vec![] })
}
"
    .to_string()
}

fn gen_writer_rs() -> String {
    r"use rapidbyte_sdk::prelude::*;
use crate::config::Config;

pub async fn write_stream(config: &Config, ctx: &Context, stream: &StreamContext) -> Result<WriteSummary, ConnectorError> {
    // TODO: Implement stream writing
    let _ = (config, ctx, stream);
    Ok(WriteSummary {
        records_written: 0,
        bytes_written: 0,
        batches_written: 0,
        checkpoint_count: 0,
        records_failed: 0,
        perf: None,
    })
}
"
    .to_string()
}
