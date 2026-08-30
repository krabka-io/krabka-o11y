use super::{Deserialize, KeyValue, Serialize};

/// A span event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub time_unix_nano: i64,
    pub name: String,
    pub attrs: Vec<KeyValue>,
}
