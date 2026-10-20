use krabka_units::convert::TimeExt as _;

use crate::tests::prelude::{
    BlockIndex, DistributorError, HttpQueryError, LabelIndex, Labels, Time, check,
};

/// `parse_decimal_sample_literal` reads a decimal literal as an EXACT
/// rational rather than a float, which is the whole point: 0.1 has no
/// float representation, and a sample that round-trips through one comes
/// back as 0.100000000000000005551. The denominator is the power of ten
/// the fraction needed, so the pair is returned unreduced.
///
/// The exponent shifts that power either way, and the two directions take
/// different branches -- a negative shift multiplies the numerator, a
/// positive one raises the denominator -- so both are checked.
///
/// Two mutations here are equivalent rather than untested. The branch test
/// `decimal_places >= 0` could be `> 0`: at zero both paths raise ten to
/// the zeroth and leave the numerator alone. And the early refusal of a
/// second exponent marker is a fast path only -- `parse_decimal_sample_
/// exponent` calls `parse::<i32>()`, which rejects anything containing an
/// `e` anyway. Both are pinned by behaviour that cannot distinguish them.
#[test]
pub(crate) fn a_decimal_sample_literal_parses_to_an_exact_rational() {
    let parse = crate::tests::prelude::parse_decimal_sample_literal;
    let is_ratio = |literal: &str, numerator: i128, denominator: u128| {
        parse(literal)
            .is_some_and(|ratio| ratio.numerator == numerator && ratio.denominator == denominator)
    };

    // Whole numbers and plain decimals, unreduced.
    check!(is_ratio("1", 1, 1));
    check!(is_ratio("0", 0, 1));
    check!(is_ratio("1.5", 15, 10), "unreduced: not (3, 2)");
    check!(is_ratio("0.1", 1, 10), "exact, where a float is not");
    check!(is_ratio("12.345", 12_345, 1_000));

    // Signs, on either spelling.
    check!(is_ratio("-1.5", -15, 10));
    check!(is_ratio("+1.5", 15, 10));
    check!(is_ratio("-0", 0, 1));

    // A missing side of the point is allowed as long as one side is there.
    check!(is_ratio(".5", 5, 10));
    check!(is_ratio("5.", 5, 1));
    check!(parse(".").is_none(), "but not both missing");

    // A positive exponent cancels decimal places and can go past them,
    // which switches branches: the numerator is scaled instead.
    check!(is_ratio("1e3", 1_000, 1));
    check!(is_ratio("1.5e2", 150, 1), "past the decimal places");
    check!(is_ratio("1.5e1", 15, 1), "exactly cancelling them");
    check!(is_ratio("1.25e1", 125, 10), "partially cancelling them");

    // A negative exponent adds places, raising the denominator.
    check!(is_ratio("1e-3", 1, 1_000));
    check!(is_ratio("1.5e-2", 15, 1_000));
    check!(
        is_ratio("15E-1", 15, 10),
        "the exponent marker is either case"
    );

    // Refusals: nothing to parse, or not a number.
    check!(parse("").is_none());
    check!(parse("-").is_none());
    check!(parse("abc").is_none());
    check!(
        parse("1.2.3").is_none(),
        "a second point is part of the fraction"
    );
    check!(parse("1e2e3").is_none(), "and a second exponent is refused");
    check!(parse("1e").is_none());
    check!(
        parse(" 1").is_none(),
        "no trimming: whitespace is not a digit"
    );
}

/// `parse_log_level_param` accepts the four levels and refuses everything
/// else BY NAME, so the caller can tell "you sent a level I do not know"
/// from "you sent no level". It returns on the first `log_level` it finds,
/// which is what decides precedence when the handler merges two sources.
#[test]
pub(crate) fn a_log_level_parameter_names_why_it_was_refused() {
    let parse = |query: &str| crate::tests::prelude::parse_log_level_param(Some(query));

    for level in ["debug", "info", "warn", "error"] {
        check!(parse(&format!("log_level={level}")).ok().as_deref() == Some(level));
    }

    // The first occurrence wins, which the handler relies on.
    check!(parse("log_level=info&log_level=warn").ok().as_deref() == Some("info"));
    // And other parameters are skipped rather than ending the search.
    check!(parse("other=1&log_level=warn").ok().as_deref() == Some("warn"));
    check!(parse("log_level=warn&other=1").ok().as_deref() == Some("warn"));

    // Percent and plus escapes are decoded before matching, in the key as
    // well as the value.
    check!(parse("log%5Flevel=warn").ok().as_deref() == Some("warn"));

    // The two refusals are distinct: an unrecognised level names what was
    // sent, a missing one says the parameter was absent.
    check!(matches!(
        parse("log_level=verbose"),
        Err(HttpQueryError::InvalidQueryParameter {
            name: "log_level",
            ..
        })
    ));
    check!(
        matches!(
            parse("log_level="),
            Err(HttpQueryError::InvalidQueryParameter { .. }),
        ),
        "an empty value is an unrecognised level, not an absent parameter"
    );
    check!(matches!(
        parse("other=1"),
        Err(HttpQueryError::MissingQueryParameter("log_level"))
    ));
    check!(matches!(
        parse(""),
        Err(HttpQueryError::MissingQueryParameter("log_level"))
    ));
    check!(matches!(
        crate::tests::prelude::parse_log_level_param(None),
        Err(HttpQueryError::MissingQueryParameter("log_level"))
    ));

    // Case matters: the levels are lower-case spellings.
    check!(parse("log_level=DEBUG").is_err());
}

/// `requested_log_level` accepts the level in a query string, a form body, or
/// both. When both carry one the BODY wins, because the merged string puts
/// it first and the parser returns on the first match -- an ordering that
/// only shows when the two disagree.
#[test]
pub(crate) fn a_log_level_post_prefers_the_body_over_the_query_string() {
    let requested = |query: Option<&str>, body: &str| {
        crate::tests::prelude::requested_log_level(
            query,
            &axum::body::Bytes::from(body.to_string()),
        )
    };

    // Either source alone.
    check!(requested(Some("log_level=debug"), "").expect("a level is named") == "debug");
    check!(requested(None, "log_level=info").expect("a level is named") == "info");

    // Both, disagreeing: the body wins.
    check!(
        requested(Some("log_level=warn"), "log_level=info").expect("a level is named") == "info"
    );

    // A body that carries no level at all, alongside a query string that
    // does. Every case above has the level in the body whenever the body is
    // non-empty, so the merge could have dropped the query string entirely
    // and they would all still pass.
    check!(requested(Some("log_level=debug"), "other=1").expect("a level is named") == "debug");

    // An empty query string alongside a body is not a source.
    check!(requested(Some(""), "log_level=error").expect("a level is named") == "error");

    // Neither source, and an unrecognised level, are refused distinctly.
    check!(let Err(HttpQueryError::MissingQueryParameter("log_level")) = requested(None, ""));
    check!(
        let Err(HttpQueryError::InvalidQueryParameter { name: "log_level", .. }) =
            requested(Some("log_level=verbose"), "")
    );
    check!(
        requested(Some("log_level=verbose"), "")
            .expect_err("an unrecognised level is refused")
            .to_string()
            .contains("verbose")
    );
}

/// `ScalarSample` holds a rational, and its division normalises the sign
/// so the denominator stays positive -- a negative divisor moves its sign
/// to the numerator rather than leaving the pair in a form the rest of the
/// type does not expect. Both signs are checked on each side.
///
/// Division and power also refuse rather than produce a nonsense value:
/// dividing by zero has no answer, and a negative base to a fractional
/// power is NaN, which must not reach a series as a sample.
#[test]
pub(crate) fn scalar_division_and_power_refuse_what_has_no_answer() {
    let scalar = crate::tests::prelude::ScalarSample::new;
    let value = |result: Option<crate::tests::prelude::ScalarSample>| {
        result.and_then(crate::tests::prelude::ScalarSample::to_f64)
    };

    // Exact division, and a repeating fraction held as a rational rather
    // than rounded on the way in.
    check!(value(scalar(6, 1).divide(scalar(3, 1))) == Some(2.0));
    check!(value(scalar(1, 1).divide(scalar(3, 1))) == Some(1.0 / 3.0));
    check!(value(scalar(0, 1).divide(scalar(5, 1))) == Some(0.0));

    // Sign normalisation: a negative divisor, a negative dividend, and
    // both. Only the last returns to positive.
    check!(value(scalar(4, 1).divide(scalar(-2, 1))) == Some(-2.0));
    check!(value(scalar(-4, 1).divide(scalar(2, 1))) == Some(-2.0));
    check!(value(scalar(-4, 1).divide(scalar(-2, 1))) == Some(2.0));

    // Dividing by zero has no answer, whatever the dividend.
    check!(scalar(1, 1).divide(scalar(0, 1)).is_none());
    check!(scalar(0, 1).divide(scalar(0, 1)).is_none());
    check!(scalar(-1, 1).divide(scalar(0, 1)).is_none());

    // Powers, including the ones that are easy to get backwards.
    check!(value(scalar(2, 1).power(scalar(3, 1))) == Some(8.0));
    check!(
        value(scalar(3, 1).power(scalar(2, 1))) == Some(9.0),
        "not the other way round"
    );
    check!(value(scalar(2, 1).power(scalar(-1, 1))) == Some(0.5));
    check!(
        value(scalar(4, 1).power(scalar(1, 2))) == Some(2.0),
        "a fractional exponent"
    );
    check!(value(scalar(5, 1).power(scalar(0, 1))) == Some(1.0));

    // A negative base to a fractional power is NaN, which must be refused
    // rather than carried into a series as a sample.
    check!(scalar(-4, 1).power(scalar(1, 2)).is_none());
}

/// The dynamic index caches hand back an entry only while it is fresh, and
/// EVICT a stale one on the way past rather than leaving it to be found
/// again. That eviction is the part worth pinning: a cache that returns
/// None but keeps the entry grows without bound for any key queried after
/// it expires.
///
/// A zero TTL reaches the stale branch without sleeping -- any elapsed
/// time at all is more than none. The boundary itself, an entry exactly at
/// its TTL, is not reachable against a real clock and is not attempted.
#[test]
pub(crate) fn a_stale_dynamic_index_entry_is_evicted_rather_than_just_missed() {
    #[derive(Debug, PartialEq)]
    struct CacheCounts {
        manifest_entries: usize,
        shard_indexes: usize,
    }

    let fresh = crate::tests::prelude::DynamicIndexCache {
        cache_ttl: krabka_units::hours(1),
        shard_cache_ttl: krabka_units::hours(1),
        ..crate::tests::prelude::DynamicIndexCache::default()
    };
    let stale = crate::tests::prelude::DynamicIndexCache {
        cache_ttl: Time::ZERO,
        shard_cache_ttl: Time::ZERO,
        ..crate::tests::prelude::DynamicIndexCache::default()
    };
    let key = || crate::tests::prelude::DynamicIndexCacheKey::TenantManifest {
        tenant: "tenant".to_string(),
    };
    let shard_key = || crate::tests::prelude::DynamicShardIndexCacheKey {
        tenant: "tenant".to_string(),
        start_ns: 0,
        end_ns: 10,
    };
    let held = |cache: &crate::tests::prelude::DynamicIndexCache| CacheCounts {
        manifest_entries: cache.entries.lock().expect("the cache lock is held").len(),
        shard_indexes: cache
            .shard_indexes
            .lock()
            .expect("the shard cache lock is held")
            .len(),
    };

    // Within the TTL: found, and still held afterwards.
    fresh.insert(key(), LabelIndex::default(), BlockIndex::default());
    fresh.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
    check!(fresh.get(&key()).is_some());
    check!(fresh.get_shard_index(&shard_key()).is_some());
    check!(
        held(&fresh)
            == (CacheCounts {
                manifest_entries: 1,
                shard_indexes: 1
            }),
        "a fresh hit leaves the entry in place"
    );

    // Past the TTL: a miss, and the entry is gone rather than merely
    // ignored -- so a second lookup finds nothing to evict.
    stale.insert(key(), LabelIndex::default(), BlockIndex::default());
    stale.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
    check!(
        held(&stale)
            == (CacheCounts {
                manifest_entries: 1,
                shard_indexes: 1
            }),
        "inserted"
    );
    check!(stale.get(&key()).is_none());
    check!(stale.get_shard_index(&shard_key()).is_none());
    check!(
        held(&stale)
            == (CacheCounts {
                manifest_entries: 0,
                shard_indexes: 0
            }),
        "and evicted on the way past"
    );

    // A key that was never inserted is a miss without disturbing anything.
    check!(
        fresh
            .get(
                &crate::tests::prelude::DynamicIndexCacheKey::TenantManifest {
                    tenant: "other".to_string(),
                }
            )
            .is_none()
    );
    check!(
        held(&fresh)
            == (CacheCounts {
                manifest_entries: 1,
                shard_indexes: 1
            }),
        "an absent key evicts nothing"
    );

    // `clear` drops all three maps at once. It is what a configuration
    // reload calls, so with its body gone the querier keeps answering from
    // indexes built for the configuration it just replaced.
    fresh.insert(key(), LabelIndex::default(), BlockIndex::default());
    fresh.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
    fresh.insert_shard_ranges(
        crate::tests::prelude::DynamicShardRangesCacheKey {
            tenant: "tenant".to_string(),
        },
        0,
        Vec::new(),
    );
    check!(
        fresh
            .shard_ranges
            .lock()
            .expect("the shard range lock is held")
            .len()
            == 1,
        "the third map is populated too"
    );
    fresh.clear();
    check!(
        held(&fresh)
            == (CacheCounts {
                manifest_entries: 0,
                shard_indexes: 0
            }),
        "cleared"
    );
    check!(
        fresh
            .shard_ranges
            .lock()
            .expect("the shard range lock is held")
            .is_empty(),
        "including the shard ranges"
    );
    check!(fresh.get(&key()).is_none(), "and a lookup misses");

    // The two caches have their OWN durations -- five seconds and five
    // minutes by default -- so each must read its own. With both set alike
    // a lookup consulting the wrong one behaves identically, so here they
    // are opposites: the manifest expires immediately and the shard index
    // does not, then the reverse.
    let short_manifest = crate::tests::prelude::DynamicIndexCache {
        cache_ttl: Time::ZERO,
        shard_cache_ttl: krabka_units::hours(1),
        ..crate::tests::prelude::DynamicIndexCache::default()
    };
    short_manifest.insert(key(), LabelIndex::default(), BlockIndex::default());
    short_manifest.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
    check!(
        short_manifest.get(&key()).is_none(),
        "the manifest ttl is zero"
    );
    check!(
        short_manifest.get_shard_index(&shard_key()).is_some(),
        "but the shard ttl is an hour"
    );

    let short_shard = crate::tests::prelude::DynamicIndexCache {
        cache_ttl: krabka_units::hours(1),
        shard_cache_ttl: Time::ZERO,
        ..crate::tests::prelude::DynamicIndexCache::default()
    };
    short_shard.insert(key(), LabelIndex::default(), BlockIndex::default());
    short_shard.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
    check!(
        short_shard.get(&key()).is_some(),
        "the manifest ttl is an hour"
    );
    check!(
        short_shard.get_shard_index(&shard_key()).is_none(),
        "but the shard ttl is zero"
    );
}

/// Both ends of the `Loki` ingestion window are strict comparisons: a
/// timestamp exactly at the oldest or the newest acceptable value is
/// accepted. That is the only input separating `<` from `<=`, and against
/// a wall clock it is unreachable -- `now` advances between choosing the
/// timestamp and the function reading it. Hence the `_at` seam, which
/// takes `now` rather than reading it.
#[test]
pub(crate) fn the_loki_ingestion_window_accepts_its_own_boundaries() {
    use krabka_units::{hours, nanos};

    let now = 1_000_000_000_000_i64;
    let labels = Labels::default();
    let check_at = |timestamp: i64, max_age, grace| {
        crate::tests::prelude::validate_loki_timestamp_window_at(
            timestamp, now, &labels, max_age, grace,
        )
    };
    let hour_ns = hours(1).nanos_i64();

    // Exactly at the oldest acceptable timestamp: accepted. One
    // nanosecond older: refused.
    check!(check_at(now - hour_ns, hours(1), Time::ZERO).is_ok());
    check!(check_at(now - hour_ns + 1, hours(1), Time::ZERO).is_ok());
    check!(check_at(now - hour_ns - 1, hours(1), Time::ZERO).is_err());

    // Exactly at the newest acceptable timestamp: accepted. One
    // nanosecond newer: refused.
    check!(check_at(now + hour_ns, Time::ZERO, hours(1)).is_ok());
    check!(check_at(now + hour_ns - 1, Time::ZERO, hours(1)).is_ok());
    check!(check_at(now + hour_ns + 1, Time::ZERO, hours(1)).is_err());

    // A bound of zero imposes nothing, and the two are independent: an
    // ancient timestamp passes with no max age, and a far-future one
    // passes with no grace period.
    check!(check_at(0, Time::ZERO, hours(1)).is_ok());
    check!(check_at(i64::MAX / 2, hours(1), Time::ZERO).is_ok());
    check!(check_at(0, Time::ZERO, Time::ZERO).is_ok());
    check!(check_at(i64::MAX, Time::ZERO, Time::ZERO).is_ok());

    // A one-nanosecond window admits the instant itself and nothing else.
    // Zero is the sentinel for "no window", so the tightest real window
    // that can be expressed is a nanosecond wide.
    check!(check_at(now, nanos(1), nanos(1)).is_ok());
    check!(check_at(now - 2, nanos(1), Time::ZERO).is_err());
    check!(check_at(now + 2, Time::ZERO, nanos(1)).is_err());

    // The refusals name their own direction rather than sharing one error.
    check!(matches!(
        check_at(now - hour_ns - 1, hours(1), Time::ZERO),
        Err(DistributorError::TimestampTooOld { .. })
    ));
    check!(matches!(
        check_at(now + hour_ns + 1, Time::ZERO, hours(1)),
        Err(DistributorError::TimestampTooNew { .. })
    ));
}
