use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(group = "gitstratum.io", version = "v1", kind = "PackCachePolicy")]
#[kube(namespaced)]
#[kube(
    shortname = "pcp",
    printcolumn = r#"{"name":"Precompute","type":"boolean","jsonPath":".spec.precomputeEnabled"}"#,
    printcolumn = r#"{"name":"TTL","type":"integer","jsonPath":".spec.ttlSeconds"}"#,
    printcolumn = r#"{"name":"Max Size","type":"integer","jsonPath":".spec.maxPackSizeBytes"}"#,
    printcolumn = r#"{"name":"Priority","type":"integer","jsonPath":".spec.priority"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PackCachePolicySpec {
    pub selector: LabelSelector,
    pub precompute_enabled: bool,
    #[serde(default)]
    pub precompute_schedule: Option<String>,
    pub ttl_seconds: u64,
    pub max_pack_size_bytes: u64,
    pub priority: i32,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelector {
    #[serde(default)]
    pub match_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub match_expressions: Vec<LabelSelectorRequirement>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelectorRequirement {
    pub key: String,
    pub operator: LabelSelectorOperator,
    #[serde(default)]
    pub values: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub enum LabelSelectorOperator {
    In,
    NotIn,
    Exists,
    DoesNotExist,
}

impl LabelSelectorOperator {
    pub fn as_str(&self) -> &'static str {
        match self {
            LabelSelectorOperator::In => "In",
            LabelSelectorOperator::NotIn => "NotIn",
            LabelSelectorOperator::Exists => "Exists",
            LabelSelectorOperator::DoesNotExist => "DoesNotExist",
        }
    }
}

impl LabelSelector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.match_labels.insert(key.into(), value.into());
        self
    }

    pub fn with_expression(mut self, requirement: LabelSelectorRequirement) -> Self {
        self.match_expressions.push(requirement);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.match_labels.is_empty() && self.match_expressions.is_empty()
    }

    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        for (key, expected_value) in &self.match_labels {
            match labels.get(key) {
                Some(actual_value) if actual_value == expected_value => continue,
                _ => return false,
            }
        }

        for expr in &self.match_expressions {
            let label_value = labels.get(&expr.key);
            let matches = match expr.operator {
                LabelSelectorOperator::In => label_value.map_or(false, |v| expr.values.contains(v)),
                LabelSelectorOperator::NotIn => {
                    label_value.map_or(true, |v| !expr.values.contains(v))
                }
                LabelSelectorOperator::Exists => label_value.is_some(),
                LabelSelectorOperator::DoesNotExist => label_value.is_none(),
            };
            if !matches {
                return false;
            }
        }

        true
    }
}

impl PackCachePolicySpec {
    pub fn is_precompute_scheduled(&self) -> bool {
        self.precompute_enabled && self.precompute_schedule.is_some()
    }

    pub fn effective_ttl(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.ttl_seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_selector_default() {
        let selector = LabelSelector::default();
        assert!(selector.match_labels.is_empty());
        assert!(selector.match_expressions.is_empty());
        assert!(selector.is_empty());
    }

    #[test]
    fn test_label_selector_with_label() {
        let selector = LabelSelector::new()
            .with_label("app", "gitstratum")
            .with_label("tier", "frontend");

        assert_eq!(selector.match_labels.len(), 2);
        assert_eq!(
            selector.match_labels.get("app"),
            Some(&"gitstratum".to_string())
        );
        assert_eq!(
            selector.match_labels.get("tier"),
            Some(&"frontend".to_string())
        );
        assert!(!selector.is_empty());
    }

    #[test]
    fn test_label_selector_with_expression() {
        let selector = LabelSelector::new().with_expression(LabelSelectorRequirement {
            key: "environment".to_string(),
            operator: LabelSelectorOperator::In,
            values: vec!["prod".to_string(), "staging".to_string()],
        });

        assert_eq!(selector.match_expressions.len(), 1);
        assert!(!selector.is_empty());
    }

    #[test]
    fn test_label_selector_matches_labels() {
        let selector = LabelSelector::new()
            .with_label("app", "gitstratum")
            .with_label("tier", "frontend");

        let mut matching_labels = BTreeMap::new();
        matching_labels.insert("app".to_string(), "gitstratum".to_string());
        matching_labels.insert("tier".to_string(), "frontend".to_string());
        matching_labels.insert("extra".to_string(), "value".to_string());

        assert!(selector.matches(&matching_labels));

        let mut non_matching_labels = BTreeMap::new();
        non_matching_labels.insert("app".to_string(), "other".to_string());
        non_matching_labels.insert("tier".to_string(), "frontend".to_string());

        assert!(!selector.matches(&non_matching_labels));
    }

    #[test]
    fn test_label_selector_matches_in_operator() {
        let selector = LabelSelector::new().with_expression(LabelSelectorRequirement {
            key: "env".to_string(),
            operator: LabelSelectorOperator::In,
            values: vec!["prod".to_string(), "staging".to_string()],
        });

        let mut prod_labels = BTreeMap::new();
        prod_labels.insert("env".to_string(), "prod".to_string());
        assert!(selector.matches(&prod_labels));

        let mut staging_labels = BTreeMap::new();
        staging_labels.insert("env".to_string(), "staging".to_string());
        assert!(selector.matches(&staging_labels));

        let mut dev_labels = BTreeMap::new();
        dev_labels.insert("env".to_string(), "dev".to_string());
        assert!(!selector.matches(&dev_labels));
    }

    #[test]
    fn test_label_selector_matches_notin_operator() {
        let selector = LabelSelector::new().with_expression(LabelSelectorRequirement {
            key: "env".to_string(),
            operator: LabelSelectorOperator::NotIn,
            values: vec!["dev".to_string(), "test".to_string()],
        });

        let mut prod_labels = BTreeMap::new();
        prod_labels.insert("env".to_string(), "prod".to_string());
        assert!(selector.matches(&prod_labels));

        let mut dev_labels = BTreeMap::new();
        dev_labels.insert("env".to_string(), "dev".to_string());
        assert!(!selector.matches(&dev_labels));

        let empty_labels = BTreeMap::new();
        assert!(selector.matches(&empty_labels));
    }

    #[test]
    fn test_label_selector_matches_exists_operator() {
        let selector = LabelSelector::new().with_expression(LabelSelectorRequirement {
            key: "managed-by".to_string(),
            operator: LabelSelectorOperator::Exists,
            values: vec![],
        });

        let mut has_label = BTreeMap::new();
        has_label.insert("managed-by".to_string(), "operator".to_string());
        assert!(selector.matches(&has_label));

        let no_label = BTreeMap::new();
        assert!(!selector.matches(&no_label));
    }

    #[test]
    fn test_label_selector_matches_doesnotexist_operator() {
        let selector = LabelSelector::new().with_expression(LabelSelectorRequirement {
            key: "legacy".to_string(),
            operator: LabelSelectorOperator::DoesNotExist,
            values: vec![],
        });

        let no_legacy = BTreeMap::new();
        assert!(selector.matches(&no_legacy));

        let mut has_legacy = BTreeMap::new();
        has_legacy.insert("legacy".to_string(), "true".to_string());
        assert!(!selector.matches(&has_legacy));
    }

    #[test]
    fn test_label_selector_operator_as_str() {
        assert_eq!(LabelSelectorOperator::In.as_str(), "In");
        assert_eq!(LabelSelectorOperator::NotIn.as_str(), "NotIn");
        assert_eq!(LabelSelectorOperator::Exists.as_str(), "Exists");
        assert_eq!(LabelSelectorOperator::DoesNotExist.as_str(), "DoesNotExist");
    }

    #[test]
    fn test_pack_cache_policy_spec_serialization() {
        let spec = PackCachePolicySpec {
            selector: LabelSelector::new().with_label("tier", "hot"),
            precompute_enabled: true,
            precompute_schedule: Some("0 */6 * * *".to_string()),
            ttl_seconds: 3600,
            max_pack_size_bytes: 104_857_600,
            priority: 100,
        };

        let json = serde_json::to_string(&spec).unwrap();
        let deserialized: PackCachePolicySpec = serde_json::from_str(&json).unwrap();

        assert!(deserialized.precompute_enabled);
        assert_eq!(
            deserialized.precompute_schedule,
            Some("0 */6 * * *".to_string())
        );
        assert_eq!(deserialized.ttl_seconds, 3600);
        assert_eq!(deserialized.max_pack_size_bytes, 104_857_600);
        assert_eq!(deserialized.priority, 100);
    }

    #[test]
    fn test_pack_cache_policy_spec_is_precompute_scheduled() {
        let with_schedule = PackCachePolicySpec {
            selector: LabelSelector::default(),
            precompute_enabled: true,
            precompute_schedule: Some("0 0 * * *".to_string()),
            ttl_seconds: 3600,
            max_pack_size_bytes: 100_000_000,
            priority: 0,
        };
        assert!(with_schedule.is_precompute_scheduled());

        let without_schedule = PackCachePolicySpec {
            selector: LabelSelector::default(),
            precompute_enabled: true,
            precompute_schedule: None,
            ttl_seconds: 3600,
            max_pack_size_bytes: 100_000_000,
            priority: 0,
        };
        assert!(!without_schedule.is_precompute_scheduled());

        let disabled = PackCachePolicySpec {
            selector: LabelSelector::default(),
            precompute_enabled: false,
            precompute_schedule: Some("0 0 * * *".to_string()),
            ttl_seconds: 3600,
            max_pack_size_bytes: 100_000_000,
            priority: 0,
        };
        assert!(!disabled.is_precompute_scheduled());
    }

    #[test]
    fn test_pack_cache_policy_spec_effective_ttl() {
        let spec = PackCachePolicySpec {
            selector: LabelSelector::default(),
            precompute_enabled: false,
            precompute_schedule: None,
            ttl_seconds: 7200,
            max_pack_size_bytes: 100_000_000,
            priority: 0,
        };

        assert_eq!(spec.effective_ttl(), std::time::Duration::from_secs(7200));
    }

    #[test]
    fn test_empty_selector_matches_all() {
        let selector = LabelSelector::default();

        let empty_labels = BTreeMap::new();
        assert!(selector.matches(&empty_labels));

        let mut some_labels = BTreeMap::new();
        some_labels.insert("key".to_string(), "value".to_string());
        assert!(selector.matches(&some_labels));
    }

    #[test]
    fn test_combined_match_labels_and_expressions() {
        let selector = LabelSelector::new()
            .with_label("app", "gitstratum")
            .with_expression(LabelSelectorRequirement {
                key: "env".to_string(),
                operator: LabelSelectorOperator::In,
                values: vec!["prod".to_string()],
            });

        let mut matching = BTreeMap::new();
        matching.insert("app".to_string(), "gitstratum".to_string());
        matching.insert("env".to_string(), "prod".to_string());
        assert!(selector.matches(&matching));

        let mut wrong_app = BTreeMap::new();
        wrong_app.insert("app".to_string(), "other".to_string());
        wrong_app.insert("env".to_string(), "prod".to_string());
        assert!(!selector.matches(&wrong_app));

        let mut wrong_env = BTreeMap::new();
        wrong_env.insert("app".to_string(), "gitstratum".to_string());
        wrong_env.insert("env".to_string(), "dev".to_string());
        assert!(!selector.matches(&wrong_env));
    }
}
