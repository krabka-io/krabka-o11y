use super::BTreeMap;

/// One alert payload ready for an Alertmanager-compatible API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlertmanagerAlert {
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub starts_at_ms: i64,
    pub ends_at_ms: Option<i64>,
    pub generator_url: String,
}
