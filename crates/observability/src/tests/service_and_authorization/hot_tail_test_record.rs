use super::*;

pub(crate) fn hot_tail_test_record(timestamp_ns: i64, app: &str) -> WalLogRecord {
    let mut labels = BTreeMap::new();
    labels.insert("app".to_string(), app.to_string());
    WalLogRecord {
        tenant: "tenant".to_string(),
        labels,
        timestamp_ns,
        line: format!("line@{timestamp_ns}"),
        structured_metadata: BTreeMap::new(),
        position: None,
    }
}
