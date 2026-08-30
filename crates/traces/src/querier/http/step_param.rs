use super::{Uri, UnixNano, query_param, default_query_range_step_ns, parse_step_to_ns};

pub(crate) fn step_param(uri: &Uri, start_ns: UnixNano, end_ns: UnixNano) -> Result<i64, &'static str> {
    let Some(step) = query_param(uri, "step") else {
        // Tempo computes a default step when the client omits it; Grafana's
        // Traces Drilldown breakdown queries send no `step`. Match that instead
        // of rejecting the query.
        return Ok(default_query_range_step_ns(start_ns, end_ns));
    };
    let Some(step_ns) = parse_step_to_ns(&step) else {
        return Err("invalid step");
    };
    if step_ns <= 0 {
        return Err("step must be positive");
    }
    Ok(step_ns)
}
