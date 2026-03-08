//! Syntax highlighting for the dev shell.

use nu_ansi_term::{Color, Style};
use reedline::{Highlighter, StyledText};

/// Highlighter for SQL keywords and dot-commands.
pub(crate) struct DevHighlighter;

const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "AND", "OR", "NOT",
    "IN", "IS", "NULL", "AS", "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET", "UNION", "ALL",
    "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX", "CASE", "WHEN", "THEN", "ELSE", "END", "LIKE",
    "BETWEEN", "EXISTS", "TRUE", "FALSE", "ASC", "DESC",
];

impl Highlighter for DevHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        let mut styled = StyledText::new();

        if line.starts_with('.') {
            styled.push((Style::new().fg(Color::Green).bold(), line.to_string()));
            return styled;
        }

        // Simple word-by-word highlighting for SQL.
        let mut chars = line.chars().peekable();
        let mut current = String::new();

        while let Some(&ch) = chars.peek() {
            if ch == '\'' {
                // String literal.
                flush_word(&mut current, &mut styled);
                let mut literal = String::new();
                literal.push(chars.next().unwrap());
                while let Some(&c) = chars.peek() {
                    literal.push(chars.next().unwrap());
                    if c == '\'' {
                        break;
                    }
                }
                styled.push((Style::new().fg(Color::Yellow), literal));
            } else if ch.is_ascii_digit() && current.is_empty() {
                // Number at start of token.
                let mut num = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_ascii_digit() || c == '.' {
                        num.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                styled.push((Style::new().fg(Color::Cyan), num));
            } else if ch.is_whitespace() || "(),;*".contains(ch) {
                flush_word(&mut current, &mut styled);
                styled.push((Style::default(), chars.next().unwrap().to_string()));
            } else {
                current.push(chars.next().unwrap());
            }
        }

        flush_word(&mut current, &mut styled);
        styled
    }
}

fn flush_word(word: &mut String, styled: &mut StyledText) {
    if word.is_empty() {
        return;
    }
    let upper = word.to_uppercase();
    if SQL_KEYWORDS.contains(&upper.as_str()) {
        styled.push((Style::new().bold(), word.clone()));
    } else {
        styled.push((Style::default(), word.clone()));
    }
    word.clear();
}
