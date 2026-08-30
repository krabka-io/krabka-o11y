use super::*;

/// `max_query_range` and the `Loki` resolution cap are both inclusive: a
/// query sitting exactly on the limit is served, and only the next
/// nanosecond -- or the next point -- is refused. The boundary is the one
/// input separating `>` from `>=` in either check.
#[test]
pub(crate) fn query_range_limits_admit_the_boundary_and_refuse_the_step_past_it() {
    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_max_query_range(Time::from_nanos(1_000_000));
    for (name, end_ns, allowed) in [
        ("exactly the limit", 1_000_000_i64, true),
        ("one nanosecond past it", 1_000_001, false),
    ] {
        let range = TimeRange::new(0, end_ns).unwrap();
        check!(
            validate_query_range_limit(&state, range).is_ok() == allowed,
            "{name}"
        );
    }

    // 11 000 points at a one-millisecond step is exactly the cap.
    for (name, end_ns, allowed) in [
        ("exactly the point cap", 11_000_000_000_i64, true),
        ("one point past the cap", 11_001_000_000, false),
    ] {
        let params = QueryParams {
            query: String::new(),
            time: None,
            start: None,
            end: None,
            since: None,
            step: Some(1_000_000),
            interval: None,
            limit: None,
            direction: None,
            delay_for: None,
        };
        let range = TimeRange::new(0, end_ns).unwrap();
        check!(
            validate_loki_query_range_resolution(&params, QueryKind::Range, range).is_ok()
                == allowed,
            "{name}"
        );
    }
}
