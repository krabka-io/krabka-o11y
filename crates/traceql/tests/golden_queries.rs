use assert2::assert;
use krabka_traceql::{SearchResponse, testkit::engine};

async fn query(q: &str) -> SearchResponse {
    engine().search("t", q, 0, 10_000, 20).await.unwrap()
}

fn trace_ids(resp: &SearchResponse) -> Vec<u8> {
    resp.traces.iter().map(|t| t.trace_id[0]).collect()
}

fn span_ids(resp: &SearchResponse) -> Vec<u8> {
    let mut ids = Vec::new();
    for trace in &resp.traces {
        for set in &trace.span_sets {
            for span in &set.spans {
                ids.push(span.span_id[0]);
            }
        }
    }
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn selector_queries_match_hand_computed_traces() {
    for (q, want) in [
        ("{ .http.method = \"GET\" }", vec![1]),
        ("{ span:duration > 150 }", vec![1]),
        ("{ .name =~ \"po.*\" }", vec![1]),
    ] {
        assert!(trace_ids(&query(q).await) == want, "query: {q}");
    }
}

#[tokio::test]
async fn single_span_and_differs_from_inter_brace_and() {
    assert!(trace_ids(&query("{ .a = 1 && .b = 2 }").await) == vec![2]);
    assert!(trace_ids(&query("{ .a = 1 } && { .b = 2 }").await) == vec![1, 2]);
}

#[tokio::test]
async fn structural_operators_return_right_hand_spans() {
    for (q, want) in [
        ("{ .svc = \"a\" } >> { .svc = \"c\" }", vec![4]),
        ("{ .svc = \"c\" } << { .svc = \"a\" }", vec![1]),
        ("{ .svc = \"a\" } > { .svc = \"b\" }", vec![2, 3]),
        ("{ .svc = \"c\" } < { .svc = \"b\" }", vec![2]),
        ("{ .svc = \"b\" } ~ { .svc = \"b\" }", vec![2, 3]),
    ] {
        assert!(span_ids(&query(q).await) == want, "query: {q}");
    }
}

#[tokio::test]
async fn structural_join_is_trace_isolated() {
    let resp = query("{ .svc = \"a\" } >> { .svc = \"d\" }").await;
    assert!(trace_ids(&resp) == vec![3]);
    assert!(span_ids(&resp) == vec![2]);
}

#[tokio::test]
async fn pipeline_count_filter_matches_trace_cardinality() {
    assert!(trace_ids(&query("{ .svc = \"b\" } | count() > 1").await) == vec![1]);
    assert!(trace_ids(&query("{ .svc = \"b\" } | count() > 5").await).is_empty());
}

#[tokio::test]
async fn trace_by_id_returns_known_trace() {
    let engine = engine();
    let got = engine.trace_by_id("t", &[1; 16]).await.unwrap().unwrap();
    assert!(got.spans.len() == 4);
    assert!(engine.trace_by_id("t", &[9; 16]).await.unwrap().is_none());
}
