//! SQL transform configuration.

use rapidbyte_sdk::ConfigSchema;
use serde::Deserialize;

/// Configuration for the SQL transform plugin.
#[derive(Debug, Clone, Deserialize, ConfigSchema)]
pub struct Config {
    /// SQL query to execute against each incoming batch.
    /// Must reference the current stream name as the table name.
    pub query: String,
}

impl Config {
    /// Return the trimmed SQL query.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the query is empty or whitespace-only.
    pub fn normalized_query(&self) -> Result<String, String> {
        let query = self.query.trim();
        if query.is_empty() {
            return Err("SQL query must not be empty".to_string());
        }
        Ok(query.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn normalized_query_rejects_empty_input() {
        let config = Config {
            query: "   \n\t  ".to_string(),
        };
        assert!(config.normalized_query().is_err());
    }

    #[test]
    fn normalized_query_trims_valid_query() {
        let config = Config {
            query: "  SELECT * FROM users  ".to_string(),
        };
        assert_eq!(
            config.normalized_query().expect("query should be valid"),
            "SELECT * FROM users"
        );
    }
}
