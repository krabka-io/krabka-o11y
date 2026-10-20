// Reading a Tempo `/api/search` response, for the suites that compare one.

use serde_json::Value as JsonValue;

/// Whether any span set of `search` holds the span whose hex id is
/// `span_id_hex`.
pub fn search_contains_span_id_hex(search: &JsonValue, span_id_hex: &str) -> bool {
    search["traces"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|trace| trace["spanSets"].as_array().into_iter().flatten())
        .flat_map(|span_set| span_set["spans"].as_array().into_iter().flatten())
        .any(|span| span["spanID"].as_str() == Some(span_id_hex))
}
