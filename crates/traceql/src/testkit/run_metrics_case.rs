use super::{Case, CaseResult, InMemorySpanStore, TraceqlEngine};

pub(crate) async fn run_metrics_case(
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
    let Some(query) = case.query else {
        result.message = "missing query".into();
        return result;
    };
    let response = match engine.query_range("t", &query, 0, 10_000, 10_000).await {
        Ok(response) => response,
        Err(err) => {
            result.message = err.to_string();
            return result;
        }
    };
    let expected = case.expect_series_count.unwrap_or(0);
    let actual = response.series.len();
    if actual == expected {
        result.passed_assertions = 1;
        result.passed = true;
    } else {
        result.message = format!("series count expected {expected}, got {actual}");
    }
    result
}
