//! Context-aware tab completion for the dev shell.

use reedline::{Completer, Span, Suggestion};

/// Completer for dot-commands and table names.
pub(crate) struct DevCompleter {
    dot_commands: Vec<String>,
}

impl DevCompleter {
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
        }
    }
}

impl Completer for DevCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let prefix = &line[..pos];
        let word_start = prefix.rfind(char::is_whitespace).map_or(0, |i| i + 1);
        let word = &prefix[word_start..];

        if word.is_empty() {
            return Vec::new();
        }

        let span = Span::new(word_start, pos);

        if word.starts_with('.') {
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

        Vec::new()
    }
}
