use super::*;

pub(crate) fn hot_tail_test_record(timestamp_ns: i64, app: &str) -> WalLogRecord {
    WalLogRecord {
        tenant: "tenant".to_string(),
        labels: BTreeMap::from([("app".to_string(), app.to_string())]),
        timestamp_ns,
        line: format!("line@{timestamp_ns}"),
        structured_metadata: BTreeMap::new(),
        position: None,
    }
}
