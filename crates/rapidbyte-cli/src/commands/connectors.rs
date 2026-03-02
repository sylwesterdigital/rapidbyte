//! Connector listing subcommand (connectors).

use anyhow::Result;

/// Execute the `connectors` command: list available connectors with manifest info.
///
/// # Errors
///
/// Returns `Err` if directory scanning or manifest parsing fails.
pub fn execute() -> Result<()> {
    let dirs = rapidbyte_runtime::connector_search_dirs();

    if dirs.is_empty() {
        println!("No connector directories found.");
        println!("Set RAPIDBYTE_CONNECTOR_DIR or place connectors in ~/.rapidbyte/plugins/");
        return Ok(());
    }

    let mut found = false;

    for dir in &dirs {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().is_some_and(|e| e == "wasm") {
                found = true;
                if let Some(manifest) = rapidbyte_runtime::load_connector_manifest(&path)? {
                    let mut roles = Vec::new();
                    if manifest.roles.source.is_some() {
                        roles.push("Source");
                    }
                    if manifest.roles.destination.is_some() {
                        roles.push("Destination");
                    }
                    if manifest.roles.transform.is_some() {
                        roles.push("Transform");
                    }

                    println!(
                        "  {} ({}@{})  [{}]",
                        manifest.name,
                        manifest.id,
                        manifest.version,
                        roles.join(", "),
                    );
                    if !manifest.description.is_empty() {
                        println!("    {}", manifest.description);
                    }
                    if manifest.config_schema.is_some() {
                        println!("    Config schema: defined");
                    }
                } else {
                    let name = path.file_stem().unwrap_or_default().to_string_lossy();
                    println!("  {name}  (no manifest)");
                }
            }
        }
    }

    if !found {
        println!("No connectors found.");
        println!("Place .wasm files in ~/.rapidbyte/plugins/ or set RAPIDBYTE_CONNECTOR_DIR");
    }

    Ok(())
}
