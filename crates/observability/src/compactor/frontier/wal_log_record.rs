use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalLogRecord {
    pub tenant: String,
    pub labels: Labels,
    pub timestamp_ns: i64,
    pub line: String,
    pub structured_metadata: BTreeMap<String, String>,
    pub position: Option<WalPosition>,
}
