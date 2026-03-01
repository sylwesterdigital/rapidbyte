//! Validation transform configuration and rule compilation.
//!
//! Parses user-defined `RuleSpec` entries (not-null, regex, range, unique) from
//! JSON config and compiles them into an optimized `CompiledConfig` for evaluation.

use std::collections::BTreeMap;

use bigdecimal::BigDecimal;
use regex::Regex;
use serde::Deserialize;
use serde_json::Number;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub rules: Vec<RuleSpec>,
}

impl rapidbyte_sdk::ConfigSchema for Config {
    const SCHEMA_JSON: &'static str = r##"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Transform Validate Config",
  "description": "Rule-based data contract validation for in-flight Arrow batches",
  "type": "object",
  "required": ["rules"],
  "properties": {
    "rules": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "oneOf": [
          {
            "required": ["assert_not_null"],
            "properties": {
              "assert_not_null": {
                "oneOf": [
                  { "$ref": "#/definitions/non_empty_field" },
                  {
                    "type": "array",
                    "minItems": 1,
                    "items": { "$ref": "#/definitions/non_empty_field" }
                  }
                ]
              }
            },
            "additionalProperties": false
          },
          {
            "required": ["assert_unique"],
            "properties": {
              "assert_unique": {
                "oneOf": [
                  { "$ref": "#/definitions/non_empty_field" },
                  {
                    "type": "array",
                    "minItems": 1,
                    "items": { "$ref": "#/definitions/non_empty_field" }
                  }
                ]
              }
            },
            "additionalProperties": false
          },
          {
            "required": ["assert_regex"],
            "properties": {
              "assert_regex": {
                "oneOf": [
                  {
                    "type": "object",
                    "required": ["field", "pattern"],
                    "properties": {
                      "field": { "$ref": "#/definitions/non_empty_field" },
                      "pattern": { "type": "string", "minLength": 1 }
                    },
                    "additionalProperties": false
                  },
                  {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                      "type": "object",
                      "required": ["field", "pattern"],
                      "properties": {
                        "field": { "$ref": "#/definitions/non_empty_field" },
                        "pattern": { "type": "string", "minLength": 1 }
                      },
                      "additionalProperties": false
                    }
                  },
                  {
                    "type": "object",
                    "minProperties": 1,
                    "additionalProperties": { "type": "string", "minLength": 1 }
                  }
                ]
              }
            },
            "additionalProperties": false
          },
          {
            "required": ["assert_range"],
            "properties": {
              "assert_range": {
                "oneOf": [
                  { "$ref": "#/definitions/range_rule" },
                  {
                    "type": "array",
                    "minItems": 1,
                    "items": { "$ref": "#/definitions/range_rule" }
                  }
                ]
              }
            },
            "additionalProperties": false
          }
        ]
      }
    }
  },
  "additionalProperties": false,
  "definitions": {
    "non_empty_field": {
      "type": "string",
      "minLength": 1,
      "pattern": "\\S"
    },
    "range_rule": {
      "type": "object",
      "required": ["field"],
      "properties": {
        "field": { "$ref": "#/definitions/non_empty_field" },
        "min": { "type": "number" },
        "max": { "type": "number" }
      },
      "additionalProperties": false
    }
  }
}"##;
}

#[derive(Debug, Clone)]
pub struct CompiledConfig {
    pub rules: Vec<CompiledRule>,
}

#[derive(Debug, Clone)]
pub enum CompiledRule {
    NotNull { field: String },
    Regex { field: String, pattern: String, regex: Regex },
    Range {
        field: String,
        min: Option<NumericBound>,
        max: Option<NumericBound>,
    },
    Unique { field: String },
}

#[derive(Debug, Clone)]
pub struct NumericBound {
    pub literal: String,
    pub decimal: BigDecimal,
    pub as_f64: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum RuleSpec {
    NotNull { assert_not_null: FieldSelector },
    Regex { assert_regex: RegexSelector },
    Range { assert_range: RangeSelector },
    Unique { assert_unique: FieldSelector },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum FieldSelector {
    One(String),
    Many(Vec<String>),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum RegexSelector {
    One(RegexRule),
    Many(Vec<RegexRule>),
    Map(BTreeMap<String, String>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct RegexRule {
    pub field: String,
    pub pattern: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum RangeSelector {
    One(RangeRule),
    Many(Vec<RangeRule>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct RangeRule {
    pub field: String,
    pub min: Option<Number>,
    pub max: Option<Number>,
}

impl Config {
    /// Compile user-defined rule specs into an optimized `CompiledConfig`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the rule list is empty, a field selector is blank,
    /// a regex pattern is invalid, or a range rule has invalid or inverted bounds.
    pub fn compile(&self) -> Result<CompiledConfig, String> {
        if self.rules.is_empty() {
            return Err("rules must not be empty".to_string());
        }

        let mut compiled = Vec::new();
        for rule in &self.rules {
            match rule {
                RuleSpec::NotNull { assert_not_null } => {
                    assert_not_null.try_for_each_field(|field| {
                        compiled.push(CompiledRule::NotNull {
                            field: field.to_string(),
                        });
                        Ok(())
                    })?;
                }
                RuleSpec::Unique { assert_unique } => {
                    assert_unique.try_for_each_field(|field| {
                        compiled.push(CompiledRule::Unique {
                            field: field.to_string(),
                        });
                        Ok(())
                    })?;
                }
                RuleSpec::Regex { assert_regex } => {
                    assert_regex.try_for_each_rule(|field, pattern| {
                        if pattern.trim().is_empty() {
                            return Err(format!(
                                "regex pattern for field '{}' must not be empty",
                                field
                            ));
                        }
                        let regex = Regex::new(pattern)
                            .map_err(|e| format!("invalid regex pattern for field '{}': {e}", field))?;
                        compiled.push(CompiledRule::Regex {
                            field: field.to_string(),
                            pattern: pattern.to_string(),
                            regex,
                        });
                        Ok(())
                    })?;
                }
                RuleSpec::Range { assert_range } => {
                    assert_range.try_for_each_rule(|field, min, max| {
                        if min.is_none() && max.is_none() {
                            return Err(format!(
                                "range rule for field '{}' must set min and/or max",
                                field
                            ));
                        }
                        let min_bound = min
                            .map(|bound| NumericBound::from_number(bound, field, "min"))
                            .transpose()?;
                        let max_bound = max
                            .map(|bound| NumericBound::from_number(bound, field, "max"))
                            .transpose()?;
                        if let (Some(minimum), Some(maximum)) = (&min_bound, &max_bound) {
                            if minimum.decimal > maximum.decimal {
                                return Err(format!(
                                    "range rule for field '{}' has min > max ({} > {})",
                                    field, minimum.literal, maximum.literal
                                ));
                            }
                        }
                        compiled.push(CompiledRule::Range {
                            field: field.to_string(),
                            min: min_bound,
                            max: max_bound,
                        });
                        Ok(())
                    })?;
                }
            }
        }

        if compiled.is_empty() {
            return Err("rules must produce at least one assertion".to_string());
        }

        Ok(CompiledConfig { rules: compiled })
    }
}

fn ensure_non_empty_field(field: &str) -> Result<(), String> {
    if field.trim().is_empty() {
        Err("field name must not be empty".to_string())
    } else {
        Ok(())
    }
}

impl FieldSelector {
    fn try_for_each_field(
        &self,
        mut callback: impl FnMut(&str) -> Result<(), String>,
    ) -> Result<(), String> {
        match self {
            Self::One(field) => {
                ensure_non_empty_field(field)?;
                callback(field)
            }
            Self::Many(fields) => {
                for field in fields {
                    ensure_non_empty_field(field)?;
                    callback(field)?;
                }
                Ok(())
            }
        }
    }
}

impl RegexSelector {
    fn try_for_each_rule(
        &self,
        mut callback: impl FnMut(&str, &str) -> Result<(), String>,
    ) -> Result<(), String> {
        match self {
            Self::One(rule) => {
                ensure_non_empty_field(&rule.field)?;
                callback(&rule.field, &rule.pattern)
            }
            Self::Many(rules) => {
                for rule in rules {
                    ensure_non_empty_field(&rule.field)?;
                    callback(&rule.field, &rule.pattern)?;
                }
                Ok(())
            }
            Self::Map(map) => {
                for (field, pattern) in map {
                    ensure_non_empty_field(field)?;
                    callback(field, pattern)?;
                }
                Ok(())
            }
        }
    }
}

impl RangeSelector {
    fn try_for_each_rule(
        &self,
        mut callback: impl FnMut(&str, Option<&Number>, Option<&Number>) -> Result<(), String>,
    ) -> Result<(), String> {
        match self {
            Self::One(rule) => {
                ensure_non_empty_field(&rule.field)?;
                callback(&rule.field, rule.min.as_ref(), rule.max.as_ref())
            }
            Self::Many(rules) => {
                for rule in rules {
                    ensure_non_empty_field(&rule.field)?;
                    callback(&rule.field, rule.min.as_ref(), rule.max.as_ref())?;
                }
                Ok(())
            }
        }
    }
}

impl NumericBound {
    fn from_number(number: &Number, field: &str, bound_name: &str) -> Result<Self, String> {
        let literal = number.to_string();
        let decimal = literal.parse::<BigDecimal>().map_err(|error| {
            format!(
                "range rule for field '{}' has invalid {} value '{}': {}",
                field, bound_name, literal, error
            )
        })?;
        let as_f64 = literal.parse::<f64>().map_err(|error| {
            format!(
                "range rule for field '{}' has non-numeric {} value '{}': {}",
                field, bound_name, literal, error
            )
        })?;
        if !as_f64.is_finite() {
            return Err(format!(
                "range rule for field '{}' has non-finite {} value '{}': expected finite number",
                field, bound_name, literal
            ));
        }
        Ok(Self {
            literal,
            decimal,
            as_f64,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rapidbyte_sdk::ConfigSchema;

    #[test]
    fn compile_supports_repeat_and_shorthand_forms() {
        let config: Config = serde_json::from_value(serde_json::json!({
            "rules": [
                { "assert_not_null": "user_id" },
                { "assert_not_null": ["email", "account_id"] },
                { "assert_unique": ["order_id", "external_id"] },
                { "assert_regex": { "email": "^.+@.+$", "phone": "^[0-9-]+$" } },
                { "assert_range": [
                    { "field": "age", "min": 0, "max": 150 },
                    { "field": "score", "min": 0 }
                ]}
            ]
        }))
        .expect("config should deserialize");

        let compiled = config.compile().expect("config should compile");
        assert_eq!(compiled.rules.len(), 9);
    }

    #[test]
    fn compile_rejects_invalid_regex_and_bounds() {
        let regex_bad: Config = serde_json::from_value(serde_json::json!({
            "rules": [{ "assert_regex": { "field": "email", "pattern": "(" } }]
        }))
        .expect("config should deserialize");
        assert!(regex_bad.compile().is_err());

        let bounds_bad: Config = serde_json::from_value(serde_json::json!({
            "rules": [{ "assert_range": { "field": "age", "min": 10, "max": 1 } }]
        }))
        .expect("config should deserialize");
        assert!(bounds_bad.compile().is_err());
    }

    #[test]
    fn schema_declares_no_additional_properties() {
        let schema: serde_json::Value =
            serde_json::from_str(Config::SCHEMA_JSON).expect("schema should be valid json");
        assert_eq!(schema["additionalProperties"], serde_json::json!(false));
    }

    #[test]
    fn schema_rejects_empty_rules_array() {
        let bad: Result<Config, _> = serde_json::from_value(serde_json::json!({
            "rules": []
        }));
        assert!(bad.is_ok(), "serde shape is valid; compile enforces semantics");
        let config = bad.expect("config should deserialize");
        assert!(config.compile().is_err());
    }

    #[test]
    fn compile_rejects_empty_fields_in_repeat_selectors() {
        let bad_field: Config = serde_json::from_value(serde_json::json!({
            "rules": [{ "assert_not_null": ["ok", "  "] }]
        }))
        .expect("config should deserialize");
        assert!(bad_field.compile().is_err());

        let bad_regex_map: Config = serde_json::from_value(serde_json::json!({
            "rules": [{ "assert_regex": { "": "^.+$" } }]
        }))
        .expect("config should deserialize");
        assert!(bad_regex_map.compile().is_err());
    }
}
