use super::*;

#[test]
pub(crate) fn detected_labels_empty_query_is_match_all() {
    // Grafana's Logs Drilldown loads `detected_labels?query=` with an empty
    // query to discover every label. An empty/blank query must parse to
    // `None` (match all streams), not be handed to the LogQL parser — which
    // rejects "" with `syntax error: unexpected $end, expecting '{'`.
    for raw in ["query=", "query=%20", "query=%20%20"] {
        let params = parse_detected_labels_params(Some(raw)).unwrap();
        assert!(params.query.is_none(), "{raw}: {:?}", params.query);
    }
    // A real stream selector is still preserved.
    let params = parse_detected_labels_params(Some("query=%7Bapp%3D%22api%22%7D")).unwrap();
    assert_eq!(params.query.as_deref(), Some(r#"{app="api"}"#));
}
