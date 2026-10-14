use super::*;

struct QueryRangeLimitCase {
    name: &'static str,
    end_ns: i64,
    allowed: bool,
}

/// `max_query_range` and the `Loki` resolution cap are both inclusive: a
/// query sitting exactly on the limit is served, and only the next
/// nanosecond -- or the next point -- is refused. The boundary is the one
/// input separating `>` from `>=` in either check.
#[test]
pub(crate) fn query_range_limits_admit_the_boundary_and_refuse_the_step_past_it() {
    let state =
        QuerierState::new(".", LabelIndex::default(), BlockIndex::default()).with_limits(Limits {
            max_query_range: Time::from_nanos(1_000_000),
            ..Limits::default()
        });
    for case in [
        QueryRangeLimitCase {
            name: "exactly the limit",
            end_ns: 1_000_000_i64,
            allowed: true,
        },
        QueryRangeLimitCase {
            name: "one nanosecond past it",
            end_ns: 1_000_001,
            allowed: false,
        },
    ] {
        let range = TimeRange::new(0, case.end_ns).unwrap();
        check!(
            validate_query_range_limit(&state, range).is_ok() == case.allowed,
            "{}",
            case.name
        );
    }

    // 11 000 points at a one-millisecond step is exactly the cap.
    for case in [
        QueryRangeLimitCase {
            name: "exactly the point cap",
            end_ns: 11_000_000_000_i64,
            allowed: true,
        },
        QueryRangeLimitCase {
            name: "one point past the cap",
            end_ns: 11_001_000_000,
            allowed: false,
        },
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
        let range = TimeRange::new(0, case.end_ns).unwrap();
        check!(
            validate_loki_query_range_resolution(&params, QueryKind::Range, range).is_ok()
                == case.allowed,
            "{}",
            case.name
        );
    }
}
