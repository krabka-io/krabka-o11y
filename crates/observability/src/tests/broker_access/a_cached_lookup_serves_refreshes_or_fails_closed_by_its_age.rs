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
        snapshot: fetched.map(|seconds| CachedSnapshot {
            value: Arc::clone(&value),
            fetched_at: at(seconds),
        }),
        last_failure: failed.map(|seconds| CachedFailure {
            failed_at: at(seconds),
            reason: "down".to_string(),
        }),
    };
    let serve = || CachedLookupDecision::Serve(Arc::clone(&value));
    let unavailable = || CachedLookupDecision::Unavailable("down".to_string());

    struct CacheLookupCase {
        name: &'static str,
        lookup: CachedLookup<u8>,
        seconds: u64,
        expected: CachedLookupDecision<u8>,
    }
    let cases = [
        CacheLookupCase {
            name: "no snapshot",
            lookup: lookup(None, None),
            seconds: 0,
            expected: CachedLookupDecision::Refresh,
        },
        CacheLookupCase {
            name: "a fresh snapshot",
            lookup: lookup(Some(0), None),
            seconds: 9,
            expected: serve(),
        },
        CacheLookupCase {
            name: "a snapshot one TTL old",
            lookup: lookup(Some(0), None),
            seconds: 10,
            expected: CachedLookupDecision::Refresh,
        },
        CacheLookupCase {
            name: "a recent failure over a stale snapshot",
            lookup: lookup(Some(0), Some(30)),
            seconds: 35,
            expected: serve(),
        },
        CacheLookupCase {
            name: "a recent failure at the bound",
            lookup: lookup(Some(0), Some(55)),
            seconds: 60,
            expected: serve(),
        },
        CacheLookupCase {
            name: "a recent failure past the bound",
            lookup: lookup(Some(0), Some(55)),
            seconds: 61,
            expected: unavailable(),
        },
        CacheLookupCase {
            name: "a recent failure with no snapshot",
            lookup: lookup(None, Some(0)),
            seconds: 9,
            expected: unavailable(),
        },
        CacheLookupCase {
            name: "an old failure with no snapshot",
            lookup: lookup(None, Some(0)),
            seconds: 10,
            expected: CachedLookupDecision::Refresh,
        },
        CacheLookupCase {
            name: "an old failure over a stale snapshot",
            lookup: lookup(Some(0), Some(30)),
            seconds: 40,
            expected: CachedLookupDecision::Refresh,
        },
    ];

    for case in cases {
        check!(
            case.lookup.decide(at(case.seconds), &policy) == case.expected,
            "{}",
            case.name
        );
    }

    let mut lookup = lookup(Some(0), None);
    check!(lookup.fail("down".to_string(), at(60), &policy) == Ok(Arc::clone(&value)));
    check!(lookup.fail("down".to_string(), at(61), &policy) == Err("down".to_string()));
    check!(lookup.store(9, at(62)) == Arc::new(9));
    check!(lookup.last_failure.is_none());
}
