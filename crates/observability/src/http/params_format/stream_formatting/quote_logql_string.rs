use super::*;

pub(crate) fn quote_logql_string(value: &str) -> String {
    serde_json::to_string(value).expect("string serialization cannot fail")
}
