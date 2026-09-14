use std::collections::HashMap;

use super::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HostInfoConfig {
    pub enabled: bool,
    pub host_identifiers: Vec<String>,
    pub metric_name: String,
}

impl Default for HostInfoConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host_identifiers: vec!["k8s.node.name".into(), "host.id".into()],
            metric_name: "traces_host_info".into(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DimensionMapping {
    pub name: String,
    pub source_labels: Vec<String>,
    pub join: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchType {
    #[default]
    Strict,
    Regex,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AttributeMatch {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct FilterPolicy {
    pub match_type: MatchType,
    pub attributes: Vec<AttributeMatch>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SpanMetricsConfig {
    pub dimensions: Vec<String>,
    pub dimension_mappings: Vec<DimensionMapping>,
    pub include: Vec<FilterPolicy>,
    pub include_any: Vec<FilterPolicy>,
    pub exclude: Vec<FilterPolicy>,
    pub target_info_excluded_dimensions: Vec<String>,
    pub span_multiplier_key: Option<String>,
    pub subprocessors: HashMap<String, bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ServiceGraphsConfig {
    pub dimensions: Vec<String>,
    pub peer_attributes: Vec<String>,
    pub enable_client_server_prefix: bool,
    pub include: Vec<FilterPolicy>,
    pub include_any: Vec<FilterPolicy>,
    pub exclude: Vec<FilterPolicy>,
    pub span_multiplier_key: Option<String>,
}

impl Default for ServiceGraphsConfig {
    fn default() -> Self {
        Self {
            dimensions: Vec::new(),
            peer_attributes: vec!["peer.service".to_string()],
            enable_client_server_prefix: false,
            include: Vec::new(),
            include_any: Vec::new(),
            exclude: Vec::new(),
            span_multiplier_key: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ProcessorConfig {
    pub span_metrics: SpanMetricsConfig,
    pub service_graphs: ServiceGraphsConfig,
    pub host_info: HostInfoConfig,
}

impl SpanMetricsConfig {
    #[must_use]
    pub(crate) fn subprocessor_enabled(&self, name: &str) -> bool {
        self.subprocessors.get(name).copied().unwrap_or(true)
    }
}
