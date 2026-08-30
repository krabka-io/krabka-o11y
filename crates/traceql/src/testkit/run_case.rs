use super::*;

pub(crate) async fn run_case(engine: &TraceqlEngine<InMemorySpanStore>, case: Case) -> CaseResult {
    match case.kind.as_str() {
        "search" => run_search_case(engine, case).await,
        "metrics" => run_metrics_case(engine, case).await,
        "trace_by_id" => run_trace_by_id_case(engine, case).await,
        other => CaseResult {
            name: case.name,
            passed: false,
            passed_assertions: 0,
            total_assertions: 1,
            message: format!("unknown case kind `{other}`"),
        },
    }
}
