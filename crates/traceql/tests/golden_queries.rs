use assert2::assert;
use krabka_traceql::{SearchResponse, testkit::engine};

async fn query(q: &str) -> SearchResponse {
    engine().search("t", q, 0, 10_000, 20).await.unwrap()
}

struct QueryExpectation {
    query: &'static str,
    expected_ids: Vec<u8>,
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
    for case in [
        QueryExpectation {
            query: "{ .http.method = \"GET\" }",
            expected_ids: vec![1],
        },
        QueryExpectation {
            query: "{ span:duration > 150 }",
            expected_ids: vec![1],
        },
        QueryExpectation {
            query: "{ .name =~ \"po.*\" }",
            expected_ids: vec![1],
        },
    ] {
        assert!(
            trace_ids(&query(case.query).await) == case.expected_ids,
            "query: {}",
            case.query
        );
    }
}

#[tokio::test]
async fn single_span_and_differs_from_inter_brace_and() {
    assert!(trace_ids(&query("{ .a = 1 && .b = 2 }").await) == vec![2]);
    assert!(trace_ids(&query("{ .a = 1 } && { .b = 2 }").await) == vec![1, 2]);
}

#[tokio::test]
async fn structural_operators_return_right_hand_spans() {
    for case in [
        QueryExpectation {
            query: "{ .svc = \"a\" } >> { .svc = \"c\" }",
            expected_ids: vec![4],
        },
        QueryExpectation {
            query: "{ .svc = \"c\" } << { .svc = \"a\" }",
            expected_ids: vec![1],
        },
        QueryExpectation {
            query: "{ .svc = \"a\" } > { .svc = \"b\" }",
            expected_ids: vec![2, 3],
        },
        QueryExpectation {
            query: "{ .svc = \"c\" } < { .svc = \"b\" }",
            expected_ids: vec![2],
        },
        QueryExpectation {
            query: "{ .svc = \"b\" } ~ { .svc = \"b\" }",
            expected_ids: vec![2, 3],
        },
    ] {
        assert!(
            span_ids(&query(case.query).await) == case.expected_ids,
            "query: {}",
            case.query
        );
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
