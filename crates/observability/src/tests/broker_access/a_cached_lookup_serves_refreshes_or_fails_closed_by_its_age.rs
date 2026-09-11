use super::*;

/// Every row is one lookup state at one age, under a ten-second TTL and a
/// one-minute staleness bound. A fresh snapshot is served. An old one is
/// refreshed. After a failed refresh the broker is not asked again for one
/// TTL: the snapshot is served while it is within the bound, and without one
/// the check fails closed.
#[test]
pub(crate) fn a_cached_lookup_serves_refreshes_or_fails_closed_by_its_age() {
    let policy = policy_for_test(8);
    let start = Instant::now();
    let at = |seconds| start + Duration::from_secs(seconds);
    let value = Arc::new(7_u8);
    let lookup = |fetched: Option<u64>, failed: Option<u64>| CachedLookup {
        snapshot: fetched.map(|seconds| (Arc::clone(&value), at(seconds))),
        last_failure: failed.map(|seconds| (at(seconds), "down".to_string())),
    };
    let serve = || CachedLookupDecision::Serve(Arc::clone(&value));
    let unavailable = || CachedLookupDecision::Unavailable("down".to_string());

    let cases = [
        (
            "no snapshot",
            lookup(None, None),
            0,
            CachedLookupDecision::Refresh,
        ),
        ("a fresh snapshot", lookup(Some(0), None), 9, serve()),
        (
            "a snapshot one TTL old",
            lookup(Some(0), None),
            10,
            CachedLookupDecision::Refresh,
        ),
        (
            "a recent failure over a stale snapshot",
            lookup(Some(0), Some(30)),
            35,
            serve(),
        ),
        (
            "a recent failure at the bound",
            lookup(Some(0), Some(55)),
            60,
            serve(),
        ),
        (
            "a recent failure past the bound",
            lookup(Some(0), Some(55)),
            61,
            unavailable(),
        ),
        (
            "a recent failure with no snapshot",
            lookup(None, Some(0)),
            9,
            unavailable(),
        ),
        (
            "an old failure with no snapshot",
            lookup(None, Some(0)),
            10,
            CachedLookupDecision::Refresh,
        ),
        (
            "an old failure over a stale snapshot",
            lookup(Some(0), Some(30)),
            40,
            CachedLookupDecision::Refresh,
        ),
    ];

    for (name, lookup, seconds, expected) in cases {
        check!(lookup.decide(at(seconds), &policy) == expected, "{name}");
    }

    let mut lookup = lookup(Some(0), None);
    check!(lookup.fail("down".to_string(), at(60), &policy) == Ok(Arc::clone(&value)));
    check!(lookup.fail("down".to_string(), at(61), &policy) == Err("down".to_string()));
    check!(lookup.store(9, at(62)) == Arc::new(9));
    check!(lookup.last_failure.is_none());
}
