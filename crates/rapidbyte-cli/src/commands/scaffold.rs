//! Plugin project scaffolding subcommand (scaffold).

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

/// Plugin kind derived from the name prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Source,
    Destination,
    Transform,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Role::Source => "source",
            Role::Destination => "destination",
            Role::Transform => "transform",
        }
    }
}

/// Run the scaffold command.
///
/// # Errors
///
/// Returns `Err` if the plugin name is invalid, the output directory
/// cannot be created, or file writing fails.
#[allow(clippy::too_many_lines)] // Scaffolding requires writing many files with sequential steps.
pub fn run(name: &str, output: Option<&str>) -> Result<()> {
    // Determine role from name prefix
    let role = if name.starts_with("source-") {
        Role::Source
    } else if name.starts_with("dest-") {
        Role::Destination
    } else if name.starts_with("transform-") {
        Role::Transform
    } else {
        bail!("Plugin name must start with 'source-', 'dest-', or 'transform-', got '{name}'");
    };

    // Compute output directory
    let base_dir = if let Some(dir) = output {
        PathBuf::from(dir).join(name)
    } else {
        let kind_dir = match role {
            Role::Source => "plugins/sources",
            Role::Destination => "plugins/destinations",
            Role::Transform => "plugins/transforms",
        };
        PathBuf::from(kind_dir).join(name)
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
        match role {
            Role::Source => "Source",
            Role::Destination => "Destination",
            Role::Transform => "Transform",
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
    write_file(
        &cargo_dir.join("config.toml"),
        gen_cargo_config(),
        &mut created_files,
    )?;

    // Write build.rs
    let build_rs = gen_build_rs(name, &display_name, role, &service_name);
    write_file(&base_dir.join("build.rs"), &build_rs, &mut created_files)?;
    write_file(
        &base_dir.join("README.md"),
        &gen_readme(name, role),
        &mut created_files,
    )?;

    // Write source files
    match role {
        Role::Source => {
            write_file(
                &src_dir.join("main.rs"),
                &gen_source_main(&struct_name),
                &mut created_files,
            )?;
            write_file(&src_dir.join("config.rs"), gen_config(), &mut created_files)?;
            write_file(
                &src_dir.join("client.rs"),
                gen_client_rs(),
                &mut created_files,
            )?;
            write_file(&src_dir.join("read.rs"), gen_read_rs(), &mut created_files)?;
            write_file(
                &src_dir.join("discover.rs"),
                gen_discover_rs(),
                &mut created_files,
            )?;
        }
        Role::Destination => {
            write_file(
                &src_dir.join("main.rs"),
                &gen_dest_main(&struct_name),
                &mut created_files,
            )?;
            write_file(&src_dir.join("config.rs"), gen_config(), &mut created_files)?;
            write_file(
                &src_dir.join("client.rs"),
                gen_client_rs(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("write.rs"),
                gen_write_rs(),
                &mut created_files,
            )?;
        }
        Role::Transform => {
            write_file(
                &src_dir.join("main.rs"),
                &gen_transform_main(&struct_name),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("config.rs"),
                gen_transform_config(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("validate.rs"),
                gen_transform_validate_rs(),
                &mut created_files,
            )?;
            write_file(
                &src_dir.join("transform.rs"),
                gen_transform_rs(),
                &mut created_files,
            )?;
        }
    }

    // Print summary
    println!("Scaffolded {} plugin '{}'", role.as_str(), name);
    println!();
    println!("WARNING: generated scaffold is NOT PRODUCTION-READY.");
    println!("Replace every UNIMPLEMENTED stub before shipping or committing plugin code.");
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
            println!("  4. Implement schema discovery in src/discover.rs");
            println!("  5. Implement stream reading in src/read.rs");
        }
        Role::Destination => {
            println!("  3. Implement connection validation in src/client.rs");
            println!("  4. Implement stream writing in src/write.rs");
        }
        Role::Transform => {
            println!("  3. Replace the validation stub in src/validate.rs");
            println!("  4. Implement batch transformation in src/transform.rs");
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

/// Uppercase the first character of a string, leaving the rest unchanged.
fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => {
            let mut result = String::with_capacity(s.len());
            for uc in c.to_uppercase() {
                result.push(uc);
            }
            result.push_str(chars.as_str());
            result
        }
    }
}

/// Convert `source-mysql` -> `SourceMysql`, `dest-snowflake` -> `DestSnowflake`.
fn to_struct_name(name: &str) -> String {
    name.split('-').map(capitalize_first).collect()
}

/// Extract the service name: `source-mysql` -> `MySQL`, `dest-snowflake` -> `Snowflake`.
/// Falls back to title-casing the suffix.
fn extract_service_name(name: &str, role: Role) -> String {
    let prefix = match role {
        Role::Source => "source-",
        Role::Destination => "dest-",
        Role::Transform => "transform-",
    };
    let suffix = name.strip_prefix(prefix).unwrap_or(name);
    capitalize_first(suffix)
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
rapidbyte-sdk = {{ path = "../../../crates/rapidbyte-sdk" }}
serde = {{ version = "1", features = ["derive"] }}
serde_json = "1"
tokio = {{ version = "1.36", features = ["rt", "macros", "io-util"] }}
arrow = {{ version = "53", features = ["ipc"] }}

[build-dependencies]
rapidbyte-sdk = {{ path = "../../../crates/rapidbyte-sdk", default-features = false, features = ["build"] }}
"#
    )
}

fn gen_cargo_config() -> &'static str {
    r#"[build]
target = "wasm32-wasip2"
"#
}

fn gen_build_rs(name: &str, display_name: &str, role: Role, service_name: &str) -> String {
    match role {
        Role::Source => format!(
            r#"use rapidbyte_sdk::build::ManifestBuilder;
use rapidbyte_sdk::wire::SyncMode;

fn main() {{
    ManifestBuilder::source("rapidbyte/{name}")
        .name("{display_name}")
        .description("Source plugin for {service_name}")
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
        .description("Destination plugin for {service_name}")
        .write_modes(&[WriteMode::Append])
        .allow_runtime_network()
        .emit();
}}
"#
        ),
        Role::Transform => format!(
            r#"use rapidbyte_sdk::build::ManifestBuilder;

fn main() {{
    ManifestBuilder::transform("rapidbyte/{name}")
        .name("{display_name}")
        .description("Transform plugin for {service_name}")
        .emit();
}}
"#
        ),
    }
}

fn gen_source_main(struct_name: &str) -> String {
    format!(
        r#"// GENERATED BY rapidbyte scaffold.
// NOT PRODUCTION-READY: replace every UNIMPLEMENTED stub before shipping.

mod config;
mod client;
mod discover;
mod read;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::plugin(source)]
pub struct {struct_name} {{
    config: config::Config,
}}

impl Source for {struct_name} {{
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {{
        Ok((
            Self {{ config }},
            PluginInfo {{
                protocol_version: ProtocolVersion::V5,
                features: vec![],
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            }},
        ))
    }}

    async fn discover(&mut self, _ctx: &Context) -> Result<Catalog, PluginError> {{
        discover::discover_catalog(&self.config)
    }}

    async fn validate(config: &Self::Config, _ctx: &Context) -> Result<ValidationResult, PluginError> {{
        client::validate(config).await
    }}

    async fn read(&mut self, ctx: &Context, stream: StreamContext) -> Result<ReadSummary, PluginError> {{
        read::read_stream(&self.config, ctx, &stream).await
    }}

    async fn close(&mut self, ctx: &Context) -> Result<(), PluginError> {{
        ctx.log(LogLevel::Info, &format!("{{}}: close", env!("CARGO_PKG_NAME")));
        Ok(())
    }}
}}
"#
    )
}

fn gen_dest_main(struct_name: &str) -> String {
    format!(
        r#"// GENERATED BY rapidbyte scaffold.
// NOT PRODUCTION-READY: replace every UNIMPLEMENTED stub before shipping.

mod config;
mod client;
mod write;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::plugin(destination)]
pub struct {struct_name} {{
    config: config::Config,
}}

impl Destination for {struct_name} {{
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {{
        Ok((
            Self {{ config }},
            PluginInfo {{
                protocol_version: ProtocolVersion::V5,
                features: vec![],
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            }},
        ))
    }}

    async fn validate(config: &Self::Config, _ctx: &Context) -> Result<ValidationResult, PluginError> {{
        client::validate(config).await
    }}

    async fn write(&mut self, ctx: &Context, stream: StreamContext) -> Result<WriteSummary, PluginError> {{
        write::write_stream(&self.config, ctx, &stream).await
    }}

    async fn close(&mut self, ctx: &Context) -> Result<(), PluginError> {{
        ctx.log(LogLevel::Info, &format!("{{}}: close", env!("CARGO_PKG_NAME")));
        Ok(())
    }}
}}
"#
    )
}

fn gen_config() -> &'static str {
    r#"// GENERATED BY rapidbyte scaffold.
// NOT PRODUCTION-READY: replace every UNIMPLEMENTED stub before shipping.

use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

/// Connection config deserialized from pipeline YAML.
#[derive(Debug, Clone, Deserialize, ConfigSchema)]
pub struct Config {
    /// Hostname
    pub host: String,
    /// Port
    #[serde(default = "default_port")]
    #[schema(default = 0)]
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
    0
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
}

fn gen_transform_main(struct_name: &str) -> String {
    format!(
        r#"// GENERATED BY rapidbyte scaffold.
// NOT PRODUCTION-READY: replace every UNIMPLEMENTED stub before shipping.

mod config;
mod validate;
mod transform;

use rapidbyte_sdk::prelude::*;

#[rapidbyte_sdk::plugin(transform)]
pub struct {struct_name} {{
    config: config::Config,
}}

impl Transform for {struct_name} {{
    type Config = config::Config;

    async fn init(config: Self::Config) -> Result<(Self, PluginInfo), PluginError> {{
        Ok((
            Self {{ config }},
            PluginInfo {{
                protocol_version: ProtocolVersion::V5,
                features: vec![],
                default_max_batch_bytes: StreamLimits::DEFAULT_MAX_BATCH_BYTES,
            }},
        ))
    }}

    async fn validate(
        config: &Self::Config,
        _ctx: &Context,
    ) -> Result<ValidationResult, PluginError> {{
        validate::validate_config(config)
    }}

    async fn transform(
        &mut self,
        ctx: &Context,
        stream: StreamContext,
    ) -> Result<TransformSummary, PluginError> {{
        transform::run(ctx, &stream, &self.config).await
    }}
}}
"#
    )
}

fn gen_client_rs() -> &'static str {
    r#"// GENERATED BY rapidbyte scaffold.
// NOT PRODUCTION-READY: replace every UNIMPLEMENTED stub before shipping.

use rapidbyte_sdk::prelude::*;
use crate::config::Config;

pub async fn validate(config: &Config) -> Result<ValidationResult, PluginError> {
    let _ = config;
    Err(PluginError::config(
        "UNIMPLEMENTED",
        "Connection validation is not implemented yet".to_string(),
    ))
}
"#
}

fn gen_read_rs() -> &'static str {
    r#"// GENERATED BY rapidbyte scaffold.
// NOT PRODUCTION-READY: replace every UNIMPLEMENTED stub before shipping.

use rapidbyte_sdk::prelude::*;
use crate::config::Config;

pub async fn read_stream(config: &Config, ctx: &Context, stream: &StreamContext) -> Result<ReadSummary, PluginError> {
    let _ = (config, ctx, stream);
    Err(PluginError::internal(
        "UNIMPLEMENTED",
        "Stream reading is not implemented yet".to_string(),
    ))
}
"#
}

fn gen_discover_rs() -> &'static str {
    r#"// GENERATED BY rapidbyte scaffold.
// NOT PRODUCTION-READY: replace every UNIMPLEMENTED stub before shipping.

use rapidbyte_sdk::prelude::*;
use crate::config::Config;

pub fn discover_catalog(config: &Config) -> Result<Catalog, PluginError> {
    let _ = config;
    Err(PluginError::schema(
        "UNIMPLEMENTED",
        "Schema discovery is not implemented yet".to_string(),
    ))
}
"#
}

fn gen_write_rs() -> &'static str {
    r#"// GENERATED BY rapidbyte scaffold.
// NOT PRODUCTION-READY: replace every UNIMPLEMENTED stub before shipping.

use rapidbyte_sdk::prelude::*;
use crate::config::Config;

pub async fn write_stream(config: &Config, ctx: &Context, stream: &StreamContext) -> Result<WriteSummary, PluginError> {
    let _ = (config, ctx, stream);
    Err(PluginError::internal(
        "UNIMPLEMENTED",
        "Stream writing is not implemented yet".to_string(),
    ))
}
"#
}

fn gen_transform_config() -> &'static str {
    r#"// GENERATED BY rapidbyte scaffold.
// NOT PRODUCTION-READY: replace every UNIMPLEMENTED stub before shipping.

use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

/// Transform config deserialized from pipeline YAML.
#[derive(Debug, Clone, Deserialize, ConfigSchema)]
pub struct Config {
    /// Replace this placeholder with real transform-specific settings.
    #[serde(default)]
    pub mode: Option<String>,
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        let _ = self;
        Err("UNIMPLEMENTED: config validation is not implemented yet".to_string())
    }
}
"#
}

fn gen_transform_validate_rs() -> &'static str {
    r#"// GENERATED BY rapidbyte scaffold.
// NOT PRODUCTION-READY: replace every UNIMPLEMENTED stub before shipping.

use crate::config::Config;
use rapidbyte_sdk::prelude::*;

pub fn validate_config(config: &Config) -> Result<ValidationResult, PluginError> {
    match config.validate() {
        Ok(()) => Ok(ValidationResult {
            status: ValidationStatus::Success,
            message: "Transform config is valid".to_string(),
            warnings: Vec::new(),
        }),
        Err(message) => Ok(ValidationResult {
            status: ValidationStatus::Failed,
            message,
            warnings: vec!["Replace the scaffold validation stub before shipping".to_string()],
        }),
    }
}
"#
}

fn gen_transform_rs() -> &'static str {
    r#"// GENERATED BY rapidbyte scaffold.
// NOT PRODUCTION-READY: replace every UNIMPLEMENTED stub before shipping.

use crate::config::Config;
use rapidbyte_sdk::prelude::*;

pub async fn run(
    ctx: &Context,
    stream: &StreamContext,
    config: &Config,
) -> Result<TransformSummary, PluginError> {
    let _ = (ctx, stream, config);
    Err(PluginError::internal(
        "UNIMPLEMENTED",
        "Batch transformation is not implemented yet".to_string(),
    ))
}
"#
}

fn gen_readme(name: &str, role: Role) -> String {
    format!(
        r"# {name}

This plugin was generated by `rapidbyte scaffold` as a starting point for a {role} plugin.

## NOT PRODUCTION-READY

- This scaffold intentionally contains `UNIMPLEMENTED` stubs.
- Do not commit or ship this plugin until every stub has been replaced.
- Review the generated source files and implement validation plus connector logic before use.
",
        role = role.as_str(),
    )
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::run;

    #[test]
    fn scaffold_source_generates_explicit_unimplemented_stubs() {
        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let temp_dir_str = temp_dir
            .path()
            .to_str()
            .expect("temp dir path should be valid utf-8");

        run("source-example", Some(temp_dir_str)).expect("source scaffold should succeed");

        let root_dir = temp_dir.path().join("source-example");
        let plugin_dir = root_dir.join("src");
        let readme = fs::read_to_string(root_dir.join("README.md"))
            .expect("README.md should exist in generated source plugin");
        let config_rs = fs::read_to_string(plugin_dir.join("config.rs"))
            .expect("config.rs should exist in generated source plugin");
        let client_rs = fs::read_to_string(plugin_dir.join("client.rs"))
            .expect("client.rs should exist in generated source plugin");
        let read_rs = fs::read_to_string(plugin_dir.join("read.rs"))
            .expect("read.rs should exist in generated source plugin");
        let discover_rs = fs::read_to_string(plugin_dir.join("discover.rs"))
            .expect("discover.rs should exist in generated source plugin");

        assert!(readme.contains("NOT PRODUCTION-READY"));
        assert!(readme.contains("UNIMPLEMENTED"));
        assert!(!config_rs.contains("3306"));
        assert!(config_rs.contains("pub struct Config"));
        assert!(client_rs.contains("NOT PRODUCTION-READY"));
        assert!(client_rs.contains("PluginError::config("));
        assert!(client_rs.contains("\"UNIMPLEMENTED\""));
        assert!(read_rs.contains("NOT PRODUCTION-READY"));
        assert!(read_rs.contains("PluginError::internal("));
        assert!(read_rs.contains("\"UNIMPLEMENTED\""));
        assert!(discover_rs.contains("NOT PRODUCTION-READY"));
        assert!(discover_rs.contains("PluginError::schema("));
        assert!(discover_rs.contains("\"UNIMPLEMENTED\""));
        assert!(!client_rs.contains("ValidationStatus::Success"));
        assert!(!read_rs.contains("records_read: 0"));
    }

    #[test]
    fn scaffold_destination_generates_explicit_unimplemented_stubs() {
        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let temp_dir_str = temp_dir
            .path()
            .to_str()
            .expect("temp dir path should be valid utf-8");

        run("dest-example", Some(temp_dir_str)).expect("destination scaffold should succeed");

        let root_dir = temp_dir.path().join("dest-example");
        let plugin_dir = root_dir.join("src");
        let readme = fs::read_to_string(root_dir.join("README.md"))
            .expect("README.md should exist in generated destination plugin");
        let config_rs = fs::read_to_string(plugin_dir.join("config.rs"))
            .expect("config.rs should exist in generated destination plugin");
        let client_rs = fs::read_to_string(plugin_dir.join("client.rs"))
            .expect("client.rs should exist in generated destination plugin");
        let write_rs = fs::read_to_string(plugin_dir.join("write.rs"))
            .expect("write.rs should exist in generated destination plugin");

        assert!(readme.contains("NOT PRODUCTION-READY"));
        assert!(readme.contains("UNIMPLEMENTED"));
        assert!(!config_rs.contains("3306"));
        assert!(client_rs.contains("NOT PRODUCTION-READY"));
        assert!(client_rs.contains("PluginError::config("));
        assert!(client_rs.contains("\"UNIMPLEMENTED\""));
        assert!(write_rs.contains("NOT PRODUCTION-READY"));
        assert!(write_rs.contains("PluginError::internal("));
        assert!(write_rs.contains("\"UNIMPLEMENTED\""));
        assert!(!client_rs.contains("ValidationStatus::Success"));
        assert!(!write_rs.contains("records_written: 0"));
    }

    #[test]
    fn scaffold_transform_generates_explicit_unimplemented_stubs() {
        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let temp_dir_str = temp_dir
            .path()
            .to_str()
            .expect("temp dir path should be valid utf-8");

        run("transform-example", Some(temp_dir_str)).expect("transform scaffold should succeed");

        let root_dir = temp_dir.path().join("transform-example");
        let plugin_dir = root_dir.join("src");
        let readme = fs::read_to_string(root_dir.join("README.md"))
            .expect("README.md should exist in generated transform plugin");
        let config_rs = fs::read_to_string(plugin_dir.join("config.rs"))
            .expect("config.rs should exist in generated transform plugin");
        let main_rs = fs::read_to_string(plugin_dir.join("main.rs"))
            .expect("main.rs should exist in generated transform plugin");
        let validate_rs = fs::read_to_string(plugin_dir.join("validate.rs"))
            .expect("validate.rs should exist in generated transform plugin");
        let transform_rs = fs::read_to_string(plugin_dir.join("transform.rs"))
            .expect("transform.rs should exist in generated transform plugin");

        assert!(readme.contains("NOT PRODUCTION-READY"));
        assert!(readme.contains("UNIMPLEMENTED"));
        assert!(main_rs.contains("#[rapidbyte_sdk::plugin(transform)]"));
        assert!(main_rs.contains("validate::validate_config(config)"));
        assert!(config_rs.contains("NOT PRODUCTION-READY"));
        assert!(config_rs.contains("UNIMPLEMENTED"));
        assert!(config_rs.contains("pub fn validate(&self) -> Result<(), String>"));
        assert!(validate_rs.contains("ValidationStatus::Failed"));
        assert!(validate_rs.contains("Replace the scaffold validation stub before shipping"));
        assert!(transform_rs.contains("NOT PRODUCTION-READY"));
        assert!(transform_rs.contains("PluginError::internal("));
        assert!(transform_rs.contains("\"UNIMPLEMENTED\""));
        assert!(!transform_rs.contains("records_out: 0"));
    }
}
