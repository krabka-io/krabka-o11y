use super::*;

/// `max_entries_limit_per_query` caps the `limit` parameter a client asks
/// for. A request that names no `limit` is not checked at all, which is a
/// third answer distinct from a `limit` of zero, and zero on the limit side
/// turns the cap off.
#[test]
pub(crate) fn the_entries_limit_admits_exactly_the_count_it_names() {
    let state = |max_entries_limit_per_query: u64| {
        QuerierState::new(".", LabelIndex::default(), BlockIndex::default()).with_limits(Limits {
            max_entries_limit_per_query,
            ..Limits::unenforced()
        })
    };

    check!(
        validate_query_entries_limit(&state(0), Some(1_000_000)).is_ok(),
        "zero turns the cap off"
    );
    check!(
        validate_query_entries_limit(&state(10), None).is_ok(),
        "a request that names no limit is not checked"
    );
    check!(
        validate_query_entries_limit(&state(10), Some(10)).is_ok(),
        "exactly at the limit"
    );
    check!(matches!(
        validate_query_entries_limit(&state(10), Some(11)),
        Err(HttpQueryError::MaxEntriesLimitPerQuery { .. })
    ));

    // The message is Loki's own, and it names both numbers in Loki's order.
    let error = validate_query_entries_limit(&state(10), Some(11)).expect_err("one over");
    check!(
        error.to_string()
            == "max entries limit per query exceeded, limit > max_entries_limit_per_query (11 > 10)",
        "{error}"
    );
}
