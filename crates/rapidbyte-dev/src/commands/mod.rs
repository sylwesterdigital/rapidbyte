//! Dot-command parser for the dev shell.
//!
//! Lines starting with `.` are parsed as dot-commands.
//! Everything else is forwarded as SQL.

/// Parsed REPL input.
#[derive(Debug, PartialEq)]
pub(crate) enum Command {
    /// .source <plugin> [--key value ...]
    Source {
        plugin: String,
        args: Vec<(String, String)>,
    },
    /// .tables
    Tables,
    /// .schema <table>
    Schema { table: String },
    /// .stream <table> [--limit N]
    Stream { table: String, limit: Option<u64> },
    /// .workspace
    Workspace,
    /// .clear [table]
    Clear { table: Option<String> },
    /// .help
    Help,
    /// .quit
    Quit,
    /// Raw SQL query
    Sql(String),
}

/// Parse a line of REPL input into a Command.
///
/// Returns `None` for blank lines. Returns `Some(Err(...))` for malformed dot-commands.
pub(crate) fn parse(line: &str) -> Option<Result<Command, String>> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }

    if !trimmed.starts_with('.') {
        return Some(Ok(Command::Sql(trimmed.to_string())));
    }

    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    let cmd = parts[0].to_lowercase();

    Some(match cmd.as_str() {
        ".quit" | ".exit" | ".q" => Ok(Command::Quit),
        ".help" | ".h" => Ok(Command::Help),
        ".tables" => Ok(Command::Tables),
        ".workspace" | ".ws" => Ok(Command::Workspace),
        ".schema" => {
            if parts.len() < 2 {
                Err("Usage: .schema <table>".to_string())
            } else {
                Ok(Command::Schema {
                    table: parts[1].to_string(),
                })
            }
        }
        ".stream" => parse_stream(&parts[1..]),
        ".source" => parse_source(&parts[1..]),
        ".clear" => Ok(Command::Clear {
            table: parts.get(1).map(|s| (*s).to_string()),
        }),
        _ => Err(format!("Unknown command: {cmd}. Type .help for commands.")),
    })
}

fn parse_stream(args: &[&str]) -> Result<Command, String> {
    if args.is_empty() {
        return Err("Usage: .stream <table> [--limit N]".to_string());
    }
    let table = args[0].to_string();
    let mut limit: Option<u64> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i] {
            "--limit" => {
                i += 1;
                if i >= args.len() {
                    return Err("--limit requires a value".to_string());
                }
                limit = Some(
                    args[i]
                        .parse()
                        .map_err(|_| format!("Invalid limit: {}", args[i]))?,
                );
            }
            flag if flag.starts_with("--") => {
                return Err(format!(
                    "Unknown flag for .stream: {flag}. Usage: .stream <table> [--limit N]"
                ));
            }
            _ => {
                return Err("Usage: .stream <table> [--limit N]".to_string());
            }
        }
        i += 1;
    }
    Ok(Command::Stream { table, limit })
}

fn parse_source(args: &[&str]) -> Result<Command, String> {
    if args.is_empty() {
        return Err("Usage: .source <plugin> [--key value ...]".to_string());
    }
    let plugin = args[0].to_string();
    let mut kv_args: Vec<(String, String)> = Vec::new();
    let mut i = 1;
    while i < args.len() {
        if let Some(key) = args[i].strip_prefix("--") {
            i += 1;
            if i >= args.len() {
                return Err(format!("--{key} requires a value"));
            }
            kv_args.push((key.to_string(), args[i].to_string()));
        }
        i += 1;
    }
    Ok(Command::Source {
        plugin,
        args: kv_args,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blank_line() {
        assert!(parse("").is_none());
        assert!(parse("   ").is_none());
    }

    #[test]
    fn test_sql() {
        let cmd = parse("SELECT * FROM users").unwrap().unwrap();
        assert_eq!(cmd, Command::Sql("SELECT * FROM users".to_string()));
    }

    #[test]
    fn test_quit() {
        assert_eq!(parse(".quit").unwrap().unwrap(), Command::Quit);
        assert_eq!(parse(".exit").unwrap().unwrap(), Command::Quit);
        assert_eq!(parse(".q").unwrap().unwrap(), Command::Quit);
    }

    #[test]
    fn test_help() {
        assert_eq!(parse(".help").unwrap().unwrap(), Command::Help);
        assert_eq!(parse(".h").unwrap().unwrap(), Command::Help);
    }

    #[test]
    fn test_tables() {
        assert_eq!(parse(".tables").unwrap().unwrap(), Command::Tables);
    }

    #[test]
    fn test_workspace() {
        assert_eq!(parse(".workspace").unwrap().unwrap(), Command::Workspace);
        assert_eq!(parse(".ws").unwrap().unwrap(), Command::Workspace);
    }

    #[test]
    fn test_schema() {
        let cmd = parse(".schema users").unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Schema {
                table: "users".to_string()
            }
        );
    }

    #[test]
    fn test_schema_missing_arg() {
        assert!(parse(".schema").unwrap().is_err());
    }

    #[test]
    fn test_stream_basic() {
        let cmd = parse(".stream public.users").unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Stream {
                table: "public.users".to_string(),
                limit: None
            }
        );
    }

    #[test]
    fn test_stream_with_limit() {
        let cmd = parse(".stream users --limit 1000").unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Stream {
                table: "users".to_string(),
                limit: Some(1000)
            }
        );
    }

    #[test]
    fn test_stream_missing_arg() {
        assert!(parse(".stream").unwrap().is_err());
    }

    #[test]
    fn test_stream_rejects_unknown_flag() {
        let err = parse(".stream users --foo 1").unwrap().unwrap_err();
        assert!(err.contains("Unknown flag"));
    }

    #[test]
    fn test_stream_rejects_extra_positional_argument() {
        let err = parse(".stream users extra").unwrap().unwrap_err();
        assert!(err.contains("Usage"));
    }

    #[test]
    fn test_source() {
        let cmd = parse(".source postgres --host localhost --port 5432")
            .unwrap()
            .unwrap();
        assert_eq!(
            cmd,
            Command::Source {
                plugin: "postgres".to_string(),
                args: vec![
                    ("host".to_string(), "localhost".to_string()),
                    ("port".to_string(), "5432".to_string()),
                ],
            }
        );
    }

    #[test]
    fn test_source_missing_plugin() {
        assert!(parse(".source").unwrap().is_err());
    }

    #[test]
    fn test_clear_all() {
        let cmd = parse(".clear").unwrap().unwrap();
        assert_eq!(cmd, Command::Clear { table: None });
    }

    #[test]
    fn test_clear_one() {
        let cmd = parse(".clear users").unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Clear {
                table: Some("users".to_string())
            }
        );
    }

    #[test]
    fn test_unknown_command() {
        assert!(parse(".foobar").unwrap().is_err());
    }
}
