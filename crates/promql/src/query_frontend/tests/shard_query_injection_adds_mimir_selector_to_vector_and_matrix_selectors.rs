use super::*;

#[test]
pub(crate) fn shard_query_injection_adds_mimir_selector_to_vector_and_matrix_selectors() {
    let rewritten = query_with_shard_selector(
        r#"sum(rate(http_requests_total{job="api"}[5m])) + up"#,
        QueryShard { index: 1, total: 2 },
    )
    .unwrap();

    for needle in [
        r#"__query_shard__="1_of_2""#,
        r#"job="api""#,
        "http_requests_total",
        r#"up{__query_shard__="1_of_2"}"#,
    ] {
        assert2::assert!(rewritten.contains(needle));
    }

    let rewritten = query_with_shard_selector(
        "sum(histogram_count((h </ 2) >/ 1))",
        QueryShard { index: 1, total: 2 },
    )
    .unwrap();
    let reparsed = crate::parse_promql(&rewritten).unwrap();
    let ast = crate::serialize_promql_expr(&reparsed).unwrap();
    assert2::assert!(ast["expr"]["args"][0]["op"] == ">/");
    assert2::assert!(ast["expr"]["args"][0]["lhs"]["expr"]["op"] == "</");
    assert2::assert!(
        ast["expr"]["args"][0]["lhs"]["expr"]["lhs"]["matchers"]
            == serde_json::json!([{"name":"__query_shard__","type":"=","value":"1_of_2"}])
    );
}
