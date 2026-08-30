use super::{
    FrontendRangeQuery, PromqlError, QueryFrontendOptions, Time, TimeExt, absolute_split_window,
    check_range_resolution, push_sharded_subqueries, query_supports_frontend_sharding,
};

/// Plans query-frontend fan-out for a Prometheus range query.
///
/// Time splitting happens first. Sub-range boundaries align to absolute
/// multiples of `split_interval`, in the Mimir style. Every evaluation timestamp
/// `start + n*step` goes to the absolute split window
/// `floor(t / split_interval) * split_interval`, and the eval points in one
/// window form one sub-range `[first_eval, last_eval]`. Eval points stay on the
/// caller's step grid, so each step appears in exactly one sub-range.
///
/// Absolute alignment makes the range-result cache reusable across overlapping
/// queries. An interior split window holds the same eval points for any query
/// that shares the step phase and covers that window in full. The interior
/// sub-range, and therefore its cache key, is byte-for-byte identical even when
/// the surrounding window slides. Only the partial leading and trailing windows
/// clipped by the query bounds differ between such queries.
///
/// # Errors
///
/// Returns an error when metric input is malformed, a limit is exceeded, or the
/// backing WAL, block store, or remote endpoint fails.
pub fn plan_range_query(
    query: &str,
    start_ms: i64,
    end_ms: i64,
    step: Time,
    opts: QueryFrontendOptions,
) -> Result<Vec<FrontendRangeQuery>, PromqlError> {
    if step <= Time::ZERO {
        return Err(PromqlError::Plan(
            "query range step must be positive".into(),
        ));
    }
    if opts.split_interval <= Time::ZERO {
        return Err(PromqlError::Plan(
            "query split interval must be positive".into(),
        ));
    }
    if opts.shard_count == 0 {
        return Err(PromqlError::Plan(
            "query shard count must be positive".into(),
        ));
    }
    if start_ms > end_ms {
        return Ok(Vec::new());
    }
    check_range_resolution(start_ms, end_ms, step)?;

    let shard_count = if query_supports_frontend_sharding(query)? {
        opts.shard_count
    } else {
        1
    };
    let split_interval_ms = opts.split_interval.millis_i64();
    let step_ms = step.millis_i64();
    let mut subqueries = Vec::new();
    let mut eval = start_ms;
    // Track the open sub-range: the absolute window it belongs to plus the first
    // and last eval timestamps seen in that window.
    let mut current: Option<(i64, i64, i64)> = None;

    while eval <= end_ms {
        let window = absolute_split_window(eval, split_interval_ms);
        match current.as_mut() {
            Some((open_window, _, last)) if *open_window == window => {
                *last = eval;
            }
            _ => {
                if let Some((_, range_start, range_end)) = current.take() {
                    push_sharded_subqueries(
                        &mut subqueries,
                        query,
                        range_start,
                        range_end,
                        step,
                        shard_count,
                    );
                }
                current = Some((window, eval, eval));
            }
        }

        let Some(next_eval) = eval.checked_add(step_ms) else {
            break;
        };
        eval = next_eval;
    }

    if let Some((_, range_start, range_end)) = current {
        push_sharded_subqueries(
            &mut subqueries,
            query,
            range_start,
            range_end,
            step,
            shard_count,
        );
    }

    Ok(subqueries)
}
