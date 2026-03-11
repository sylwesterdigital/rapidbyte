//! Context-aware tab completion for the dev shell.

use std::sync::{Arc, RwLock};

use reedline::{Completer, Span, Suggestion};

#[derive(Default)]
struct CompletionContext {
    source_streams: Vec<String>,
    workspace_tables: Vec<String>,
}

/// Completer for dot-commands and relevant table names.
#[derive(Clone)]
pub(crate) struct DevCompleter {
    dot_commands: Vec<String>,
    context: Arc<RwLock<CompletionContext>>,
}

impl DevCompleter {
    #[must_use]
    pub fn new() -> Self {
        Self {
            dot_commands: vec![
                ".source".into(),
                ".tables".into(),
                ".schema".into(),
                ".stream".into(),
                ".workspace".into(),
                ".clear".into(),
                ".help".into(),
                ".quit".into(),
            ],
            context: Arc::new(RwLock::new(CompletionContext::default())),
        }
    }

    pub fn refresh(&self, source_streams: Vec<String>, workspace_tables: Vec<String>) {
        let mut context = self
            .context
            .write()
            .expect("dev completer context lock should not be poisoned");
        context.source_streams = source_streams;
        context.workspace_tables = workspace_tables;
    }

    fn complete_names(candidates: &[String], word: &str, span: Span) -> Vec<Suggestion> {
        candidates
            .iter()
            .filter(|name| word.is_empty() || name.starts_with(word))
            .map(|name| Suggestion {
                value: name.clone(),
                description: None,
                style: None,
                extra: None,
                span,
                append_whitespace: true,
            })
            .collect()
    }
}

impl Completer for DevCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let prefix = &line[..pos];
        let ends_with_whitespace = prefix.chars().last().is_some_and(char::is_whitespace);
        let word_start = if ends_with_whitespace {
            pos
        } else {
            prefix.rfind(char::is_whitespace).map_or(0, |i| i + 1)
        };
        let word = &prefix[word_start..];
        let tokens = prefix.split_whitespace().collect::<Vec<_>>();
        let span = Span::new(word_start, pos);

        if word_start == 0 && word.starts_with('.') {
            return self
                .dot_commands
                .iter()
                .filter(|c| c.starts_with(word))
                .map(|c| Suggestion {
                    value: c.clone(),
                    description: None,
                    style: None,
                    extra: None,
                    span,
                    append_whitespace: true,
                })
                .collect();
        }

        let Some(command) = tokens.first().copied() else {
            return Vec::new();
        };

        if tokens.len() > 2 || (tokens.len() == 2 && ends_with_whitespace) {
            return Vec::new();
        }

        let context = self
            .context
            .read()
            .expect("dev completer context lock should not be poisoned");

        match command {
            ".schema" | ".stream" => Self::complete_names(&context.source_streams, word, span),
            ".clear" => Self::complete_names(&context.workspace_tables, word, span),
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use reedline::Completer;

    use super::DevCompleter;

    #[test]
    fn completes_dot_commands_at_command_position() {
        let mut completer = DevCompleter::new();

        let suggestions = completer.complete(".st", 3);

        assert!(suggestions.iter().any(|s| s.value == ".stream"));
    }

    #[test]
    fn completes_stream_names_for_stream_command() {
        let mut completer = DevCompleter::new();
        completer.refresh(
            vec!["public.users".to_string(), "public.orders".to_string()],
            Vec::new(),
        );

        let suggestions = completer.complete(".stream public.u", ".stream public.u".len());

        assert!(suggestions.iter().any(|s| s.value == "public.users"));
        assert!(!suggestions.iter().any(|s| s.value == "public.orders"));
    }

    #[test]
    fn completes_workspace_tables_for_clear_command() {
        let mut completer = DevCompleter::new();
        completer.refresh(Vec::new(), vec!["users".to_string(), "orders".to_string()]);

        let suggestions = completer.complete(".clear us", ".clear us".len());

        assert!(suggestions.iter().any(|s| s.value == "users"));
        assert!(!suggestions.iter().any(|s| s.value == "orders"));
    }

    #[test]
    fn ignores_irrelevant_positions() {
        let mut completer = DevCompleter::new();
        completer.refresh(vec!["users".to_string()], vec!["users".to_string()]);

        let suggestions =
            completer.complete(".stream users --limit ", ".stream users --limit ".len());

        assert!(suggestions.is_empty());
    }
}
