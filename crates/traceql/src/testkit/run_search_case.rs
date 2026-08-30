use super::*;

pub(crate) async fn run_search_case(engine: &TraceqlEngine<InMemorySpanStore>, case: Case) -> CaseResult {
    let mut result = CaseResult {
        name: case.name,
        passed: false,
        passed_assertions: 0,
        total_assertions: 2,
        message: String::new(),
    };
    let Some(query) = case.query else {
        result.message = "missing query".into();
        return result;
    };
    let response = match engine.search("t", &query, 0, 10_000, 20).await {
        Ok(response) => response,
        Err(err) => {
            result.message = err.to_string();
            return result;
        }
    };

    let expected_trace_ids = parse_u8_list(case.expect_trace_ids.as_deref());
    let actual_trace_ids = trace_ids(&response);
    if actual_trace_ids == expected_trace_ids {
        result.passed_assertions += 1;
    } else {
        let _ = write!(
            result.message,
            "trace ids expected {expected_trace_ids:?}, got {actual_trace_ids:?}; "
        );
    }

    let expected_span_ids = parse_u8_list(case.expect_span_ids.as_deref());
    let actual_span_ids = span_ids(&response);
    if actual_span_ids == expected_span_ids {
        result.passed_assertions += 1;
    } else {
        let _ = write!(
            result.message,
            "span ids expected {expected_span_ids:?}, got {actual_span_ids:?}; "
        );
    }

    result.passed = result.passed_assertions == result.total_assertions;
    result
}
