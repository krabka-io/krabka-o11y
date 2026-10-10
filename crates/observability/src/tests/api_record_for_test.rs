use super::prelude::{BTreeMap, WalLogRecord};

/// A one-line `{app="api"}` record under `tenant`, at timestamp 1.
pub(crate) fn api_record_for_test(tenant: &str) -> WalLogRecord {
    WalLogRecord {
        tenant: tenant.to_string(),
        labels: BTreeMap::from([("app".to_string(), "api".to_string())]),
        timestamp_ns: 1,
        line: "line".to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    }
}
