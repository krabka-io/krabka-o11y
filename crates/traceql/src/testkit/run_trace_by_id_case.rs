use super::{Case, CaseResult, InMemorySpanStore, TraceqlEngine};

pub(crate) async fn run_trace_by_id_case(
    engine: &TraceqlEngine<InMemorySpanStore>,
    case: Case,
) -> CaseResult {
    let mut result = CaseResult {
        name: case.name,
        passed: false,
        passed_assertions: 0,
        total_assertions: 1,
        message: String::new(),
    };
    let Some(trace_id) = case.trace_id else {
        result.message = "missing trace_id".into();
        return result;
    };
    let response = match engine.trace_by_id("t", &[trace_id; 16]).await {
        Ok(response) => response,
        Err(err) => {
            result.message = err.to_string();
            return result;
        }
    };
    let expected = case.expect_span_count.unwrap_or(0);
    let actual = response.map_or(0, |trace| trace.spans.len());
    if actual == expected {
        result.passed_assertions = 1;
        result.passed = true;
    } else {
        result.message = format!("span count expected {expected}, got {actual}");
    }
    result
}
