use krabka_units::convert::TimeExt as _;

use super::prelude::{
    BlockIndex, DistributorError, HttpQueryError, LabelIndex, Labels, Time, check,
};
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
        super::prelude::validate_loki_timestamp_window_at(timestamp, now, &labels, max_age, grace)
    };
    let hour_ns = hours(1).nanos_i64();

    // Exactly at the oldest acceptable timestamp: accepted. One
    // nanosecond older: refused.
    check!(check_at(now - hour_ns, Some(hours(1)), None).is_ok());
    check!(check_at(now - hour_ns + 1, Some(hours(1)), None).is_ok());
    check!(check_at(now - hour_ns - 1, Some(hours(1)), None).is_err());

    // Exactly at the newest acceptable timestamp: accepted. One
    // nanosecond newer: refused.
    check!(check_at(now + hour_ns, None, Some(hours(1))).is_ok());
    check!(check_at(now + hour_ns - 1, None, Some(hours(1))).is_ok());
    check!(check_at(now + hour_ns + 1, None, Some(hours(1))).is_err());

    // A bound that is absent imposes nothing, and the two are
    // independent: an ancient timestamp passes with no max age, and a
    // far-future one passes with no grace period.
    check!(check_at(0, None, Some(hours(1))).is_ok());
    check!(check_at(i64::MAX / 2, Some(hours(1)), None).is_ok());
    check!(check_at(0, None, None).is_ok());
    check!(check_at(i64::MAX, None, None).is_ok());

    // A zero window admits only the instant itself.
    check!(check_at(now, Some(nanos(0)), Some(nanos(0))).is_ok());
    check!(check_at(now - 1, Some(nanos(0)), None).is_err());
    check!(check_at(now + 1, None, Some(nanos(0))).is_err());

    // The refusals name their own direction rather than sharing one error.
    check!(matches!(
        check_at(now - hour_ns - 1, Some(hours(1)), None),
        Err(DistributorError::TimestampTooOld { .. })
    ));
    check!(matches!(
        check_at(now + hour_ns + 1, None, Some(hours(1))),
        Err(DistributorError::TimestampTooNew { .. })
    ));
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
    let scalar = super::prelude::ScalarSample::new;
    let value = |result: Option<super::prelude::ScalarSample>| {
        result.and_then(super::prelude::ScalarSample::to_f64)
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

/// `parse_log_level_param` accepts the four levels and refuses everything
/// else BY NAME, so the caller can tell "you sent a level I do not know"
/// from "you sent no level". It returns on the first `log_level` it finds,
/// which is what decides precedence when the handler merges two sources.
#[test]
pub(crate) fn a_log_level_parameter_names_why_it_was_refused() {
    let parse = |query: &str| super::prelude::parse_log_level_param(Some(query));

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
        super::prelude::parse_log_level_param(None),
        Err(HttpQueryError::MissingQueryParameter("log_level"))
    ));

    // Case matters: the levels are lower-case spellings.
    check!(parse("log_level=DEBUG").is_err());
}

/// `log_level_post` accepts the level in a query string, a form body, or
/// both. When both carry one the BODY wins, because the merged string puts
/// it first and the parser returns on the first match -- an ordering that
/// only shows when the two disagree.
#[tokio::test]
pub(crate) async fn a_log_level_post_prefers_the_body_over_the_query_string() {
    use axum::response::IntoResponse as _;

    let post = |query: Option<&str>, body: &str| {
        let query = query.map(str::to_string);
        let body = axum::body::Bytes::from(body.to_string());
        async move {
            let response = super::prelude::log_level_post(axum::extract::RawQuery(query), body)
                .await
                .into_response();
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("the response body is readable");
            (status, String::from_utf8(bytes.to_vec()).expect("utf-8"))
        }
    };

    // Either source alone.
    let (status, body) = post(Some("log_level=debug"), "").await;
    check!(status == axum::http::StatusCode::OK);
    check!(body.contains("Log level set to debug"));

    let (status, body) = post(None, "log_level=info").await;
    check!(status == axum::http::StatusCode::OK);
    check!(body.contains("Log level set to info"));

    // Both, disagreeing: the body wins.
    let (status, body) = post(Some("log_level=warn"), "log_level=info").await;
    check!(status == axum::http::StatusCode::OK);
    check!(
        body.contains("Log level set to info"),
        "the body's level, not the query string's: {body}"
    );

    // A body that carries no level at all, alongside a query string that
    // does. Every case above has the level in the body whenever the body is
    // non-empty, so the merge could have dropped the query string entirely
    // and they would all still pass.
    let (status, body) = post(Some("log_level=debug"), "other=1").await;
    check!(status == axum::http::StatusCode::OK);
    check!(
        body.contains("Log level set to debug"),
        "the query string supplies what the body lacks: {body}"
    );

    // An empty query string alongside a body is not a source.
    let (status, body) = post(Some(""), "log_level=error").await;
    check!(status == axum::http::StatusCode::OK);
    check!(body.contains("Log level set to error"));

    // Neither source, and an unrecognised level, are refused distinctly.
    let (status, body) = post(None, "").await;
    check!(status != axum::http::StatusCode::OK);
    check!(body.contains("unrecognized log level"));

    let (_, body) = post(Some("log_level=verbose"), "").await;
    check!(
        body.contains("verbose"),
        "the refusal names what was sent: {body}"
    );
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
    let fresh = super::prelude::DynamicIndexCache {
        cache_ttl: krabka_units::hours(1),
        shard_cache_ttl: krabka_units::hours(1),
        ..super::prelude::DynamicIndexCache::default()
    };
    let stale = super::prelude::DynamicIndexCache {
        cache_ttl: Time::ZERO,
        shard_cache_ttl: Time::ZERO,
        ..super::prelude::DynamicIndexCache::default()
    };
    let key = || super::prelude::DynamicIndexCacheKey::TenantManifest {
        tenant: "tenant".to_string(),
    };
    let shard_key = || super::prelude::DynamicShardIndexCacheKey {
        tenant: "tenant".to_string(),
        start_ns: 0,
        end_ns: 10,
    };
    let held = |cache: &super::prelude::DynamicIndexCache| {
        (
            cache.entries.lock().expect("the cache lock is held").len(),
            cache
                .shard_indexes
                .lock()
                .expect("the shard cache lock is held")
                .len(),
        )
    };

    // Within the TTL: found, and still held afterwards.
    fresh.insert(key(), LabelIndex::default(), BlockIndex::default());
    fresh.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
    check!(fresh.get(&key()).is_some());
    check!(fresh.get_shard_index(&shard_key()).is_some());
    check!(
        held(&fresh) == (1, 1),
        "a fresh hit leaves the entry in place"
    );

    // Past the TTL: a miss, and the entry is gone rather than merely
    // ignored -- so a second lookup finds nothing to evict.
    stale.insert(key(), LabelIndex::default(), BlockIndex::default());
    stale.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
    check!(held(&stale) == (1, 1), "inserted");
    check!(stale.get(&key()).is_none());
    check!(stale.get_shard_index(&shard_key()).is_none());
    check!(held(&stale) == (0, 0), "and evicted on the way past");

    // A key that was never inserted is a miss without disturbing anything.
    check!(
        fresh
            .get(&super::prelude::DynamicIndexCacheKey::TenantManifest {
                tenant: "other".to_string(),
            })
            .is_none()
    );
    check!(held(&fresh) == (1, 1), "an absent key evicts nothing");

    // `clear` drops all three maps at once. It is what a configuration
    // reload calls, so with its body gone the querier keeps answering from
    // indexes built for the configuration it just replaced.
    fresh.insert(key(), LabelIndex::default(), BlockIndex::default());
    fresh.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
    fresh.insert_shard_ranges(
        super::prelude::DynamicShardRangesCacheKey {
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
    check!(held(&fresh) == (0, 0), "cleared");
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
    let short_manifest = super::prelude::DynamicIndexCache {
        cache_ttl: Time::ZERO,
        shard_cache_ttl: krabka_units::hours(1),
        ..super::prelude::DynamicIndexCache::default()
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

    let short_shard = super::prelude::DynamicIndexCache {
        cache_ttl: krabka_units::hours(1),
        shard_cache_ttl: Time::ZERO,
        ..super::prelude::DynamicIndexCache::default()
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
    let parse = super::prelude::parse_decimal_sample_literal;

    // Whole numbers and plain decimals, unreduced.
    check!(parse("1") == Some((1, 1)));
    check!(parse("0") == Some((0, 1)));
    check!(parse("1.5") == Some((15, 10)), "unreduced: not (3, 2)");
    check!(parse("0.1") == Some((1, 10)), "exact, where a float is not");
    check!(parse("12.345") == Some((12_345, 1_000)));

    // Signs, on either spelling.
    check!(parse("-1.5") == Some((-15, 10)));
    check!(parse("+1.5") == Some((15, 10)));
    check!(parse("-0") == Some((0, 1)));

    // A missing side of the point is allowed as long as one side is there.
    check!(parse(".5") == Some((5, 10)));
    check!(parse("5.") == Some((5, 1)));
    check!(parse(".").is_none(), "but not both missing");

    // A positive exponent cancels decimal places and can go past them,
    // which switches branches: the numerator is scaled instead.
    check!(parse("1e3") == Some((1_000, 1)));
    check!(parse("1.5e2") == Some((150, 1)), "past the decimal places");
    check!(parse("1.5e1") == Some((15, 1)), "exactly cancelling them");
    check!(
        parse("1.25e1") == Some((125, 10)),
        "partially cancelling them"
    );

    // A negative exponent adds places, raising the denominator.
    check!(parse("1e-3") == Some((1, 1_000)));
    check!(parse("1.5e-2") == Some((15, 1_000)));
    check!(
        parse("15E-1") == Some((15, 10)),
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
