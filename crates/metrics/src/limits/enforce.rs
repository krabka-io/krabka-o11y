use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;
use krabka_blockstore::Labels;
use krabka_throttle::TokenBucket;
use krabka_units::prelude::*;
use num_traits::ToPrimitive;

use super::{LimitError, Limits};

#[cfg(test)]
mod tests {

    /// Eviction drops the least recently used tenant, not merely *a* tenant.
    /// A test that only checks the cap holds passes just as well when the
    /// newest is evicted instead, which would throw away the bucket in active
    /// use and keep the idle ones.
    #[test]
    fn eviction_drops_the_least_recently_used_tenant() {
        let enforcer = IngestEnforcer::with_max_rate_buckets(2);
        let limits = Limits {
            ingestion_rate: per_sec(1_000),
            ingestion_burst_size: 1_000,
            ..Limits::default()
        };
        let touch = |tenant: &str| {
            enforcer
                .check_sample_rate(&limits, tenant, 1)
                .expect("within the rate");
        };
        let holds = |tenant: &str| enforcer.sample_rate_buckets.contains_key(tenant);

        // Three tenants into a cap of two: the first touched is the one to go.
        touch("a");
        touch("b");
        touch("c");
        check!(enforcer.sample_rate_buckets.len() == 2, "the cap holds");
        check!(!holds("a"), "the least recently used went");
        check!(holds("b") && holds("c"), "the other two stayed");

        // Touching b again makes c the oldest, so the next arrival evicts c
        // rather than b -- which is what separates least-recently-used from
        // first-in-first-out.
        touch("b");
        touch("d");
        check!(enforcer.sample_rate_buckets.len() == 2);
        check!(!holds("c"), "c was the least recently used by then");
        check!(holds("b"), "b was rescued by being touched");
        check!(holds("d"));
    }

    /// `next_touch_stamp` is a logical clock: every call must hand out a value
    /// no earlier than the last, and never the same one twice. Eviction picks
    /// the least-recently-touched tenant by comparing these, so a clock that
    /// stood still would make every tenant equally old.
    #[test]
    fn touch_stamps_never_repeat_and_never_go_backwards() {
        let enforcer = IngestEnforcer::new();

        let first = enforcer.next_touch_stamp();
        let second = enforcer.next_touch_stamp();
        let third = enforcer.next_touch_stamp();

        check!(second > first, "{second} follows {first}");
        check!(third > second, "{third} follows {second}");

        // A run of them is strictly increasing, so no value is handed out
        // twice however many times it is called.
        let stamps: Vec<u64> = (0..8).map(|_| enforcer.next_touch_stamp()).collect();
        check!(
            stamps.windows(2).all(|pair| pair[1] > pair[0]),
            "not strictly increasing: {stamps:?}"
        );
        check!(stamps[7] > third);

        // Two enforcers keep their own clocks rather than sharing one.
        let other = IngestEnforcer::new();
        check!(
            other.next_touch_stamp() <= first,
            "a fresh clock starts over"
        );
    }
    use assert2::{assert, check};
    use krabka_blockstore::Labels;

    use super::*;
    use crate::limits::Limits;

    /// The per-query sample cap is off when zero and otherwise strict, so a
    /// query landing exactly on it is still served.
    #[test]
    fn the_sample_cap_admits_exactly_its_limit() {
        let limits = |max_samples_per_query| Limits {
            max_samples_per_query,
            ..Limits::default()
        };

        check!(
            QueryEnforcer::check_sample_count(&limits(0), u64::MAX).is_ok(),
            "zero is off"
        );
        check!(
            QueryEnforcer::check_sample_count(&limits(10), 10).is_ok(),
            "ten fits ten"
        );
        check!(QueryEnforcer::check_sample_count(&limits(10), 0).is_ok());

        let err = QueryEnforcer::check_sample_count(&limits(10), 11).unwrap_err();
        check!(
            matches!(
                err,
                LimitError::SamplesPerQueryExceeded {
                    limit: 10,
                    observed: 11
                }
            ),
            "got: {err:?}"
        );
    }

    /// Label length caps reject only what exceeds them, and the name and the
    /// value have separate caps, so each is checked at its own edge against a
    /// label set where the other is comfortably inside.
    #[test]
    fn label_length_caps_admit_exactly_their_limit() {
        let limits = Limits {
            max_label_name_length: krabka_units::bytes(4),
            max_label_value_length: krabka_units::bytes(5),
            ..Limits::default()
        };
        let label = |name: &str, value: &str| {
            let mut set = Labels::new();
            set.insert(name, value);
            set
        };

        check!(
            IngestEnforcer::check_labels(&limits, &label("abcd", "vwxyz")).is_ok(),
            "both at edge"
        );

        let err = IngestEnforcer::check_labels(&limits, &label("abcde", "v")).unwrap_err();
        check!(
            matches!(
                err,
                LimitError::LabelNameTooLong {
                    limit: 4,
                    observed: 5
                }
            ),
            "got: {err:?}"
        );

        let err = IngestEnforcer::check_labels(&limits, &label("ab", "vwxyz!")).unwrap_err();
        check!(
            matches!(
                err,
                LimitError::LabelValueTooLong {
                    limit: 5,
                    observed: 6
                }
            ),
            "got: {err:?}"
        );

        check!(
            IngestEnforcer::check_labels(&limits, &Labels::new()).is_ok(),
            "no labels, no limit"
        );
    }

    /// Both query caps are off when zero and otherwise reject only what
    /// exceeds them, so a query landing exactly on either is still allowed.
    /// The two are checked one at a time, since a range within the length cap
    /// can still be outside the lookback and the errors name different fields.
    #[test]
    fn query_range_caps_admit_exactly_their_limit() {
        let limits = |length_secs, lookback_secs| Limits {
            max_query_length: krabka_units::secs(length_secs),
            max_query_lookback: krabka_units::secs(lookback_secs),
            ..Limits::default()
        };
        let now = 1_000_000_i64;

        // Zero turns a cap off: an enormous range passes.
        check!(QueryEnforcer::check_range(&limits(0, 0), 0, now, now).is_ok());

        // Length cap of 10s: a 10s range fits, 10.001s does not.
        let start = now - 10_000;
        check!(QueryEnforcer::check_range(&limits(10, 0), start, now, now).is_ok());
        let err = QueryEnforcer::check_range(&limits(10, 0), start - 1, now, now).unwrap_err();
        check!(
            matches!(
                err,
                LimitError::QueryRangeTooLong {
                    limit_secs: 10,
                    observed_secs: 11
                }
            ),
            "got: {err:?}"
        );

        // Lookback cap of 10s, measured from the range start to now. The
        // length cap is off, so only the lookback can reject here.
        check!(QueryEnforcer::check_range(&limits(0, 10), start, now, now).is_ok());
        let err = QueryEnforcer::check_range(&limits(0, 10), start - 1, now, now).unwrap_err();
        check!(
            matches!(
                err,
                LimitError::QueryLookbackExceeded {
                    limit_secs: 10,
                    observed_secs: 11
                }
            ),
            "got: {err:?}"
        );

        // A range running backwards has no extent and cannot exceed anything.
        check!(QueryEnforcer::check_range(&limits(10, 10), now, now - 60_000, now).is_ok());
    }

    /// Reported seconds round *up*, so a range a millisecond over a whole
    /// second is reported as the next second rather than the one it passed.
    #[test]
    fn reported_seconds_round_up() {
        check!(secs_ceil(krabka_units::millis(0)) == 0);
        check!(
            secs_ceil(krabka_units::millis(1)) == 1,
            "any remainder rounds up"
        );
        check!(
            secs_ceil(krabka_units::millis(1_000)) == 1,
            "a whole second stays whole"
        );
        check!(secs_ceil(krabka_units::millis(1_001)) == 2);
        check!(secs_ceil(krabka_units::secs(90)) == 90);
    }

    fn limits_with(series: u64, name_len: ByteSize, val_len: ByteSize) -> Limits {
        Limits {
            max_global_series_per_user: series,
            max_label_name_length: name_len,
            max_label_value_length: val_len,
            ..Limits::default()
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert(*name, *value);
        }
        labels
    }

    #[test]
    fn active_series_cap_rejects_over_limit() {
        let e = IngestEnforcer::new();
        let l = limits_with(100, kibibytes(1), kibibytes(2));
        assert!(e.check_active_series(&l, "t", 1, 99).is_ok());
        assert!(e.check_active_series(&l, "t", 1, 100).is_err());
    }

    #[test]
    fn zero_series_cap_is_unlimited() {
        let e = IngestEnforcer::new();
        let l = limits_with(0, kibibytes(1), kibibytes(2));
        assert!(e.check_active_series(&l, "t", 1_000_000, 5_000_000).is_ok());
    }

    #[test]
    fn label_length_caps_enforced() {
        let l = limits_with(0, bytes(4), bytes(4));
        let ok = labels(&[("ab", "cd")]);
        let bad_name = labels(&[("toolong", "x")]);
        let bad_val = labels(&[("a", "toolong")]);
        check!(IngestEnforcer::check_labels(&l, &ok).is_ok());
        assert!(matches!(
            IngestEnforcer::check_labels(&l, &bad_name),
            Err(LimitError::LabelNameTooLong { .. })
        ));
        assert!(matches!(
            IngestEnforcer::check_labels(&l, &bad_val),
            Err(LimitError::LabelValueTooLong { .. })
        ));
    }

    #[test]
    fn ingestion_rate_bucket_eventually_rejects() {
        let e = IngestEnforcer::new();
        let l = Limits {
            ingestion_rate: per_sec(100),
            ingestion_burst_size: 100,
            ..Limits::default()
        };
        assert!(e.check_sample_rate(&l, "t", 100).is_ok());
        assert!(e.check_sample_rate(&l, "t", 100).is_err());
    }

    #[test]
    fn fractional_rate_still_throttles() {
        // A positive but sub-0.5 rate must not round down to the unlimited
        // (rate-0) sentinel; it clamps to >= 1 sample/sec and still throttles.
        let e = IngestEnforcer::new();
        let l = Limits {
            ingestion_rate: Frequency::from_per_sec(0.4),
            ingestion_burst_size: 1,
            ..Limits::default()
        };
        assert!(e.check_sample_rate(&l, "t", 1).is_ok());
        assert!(e.check_sample_rate(&l, "t", 1).is_err());
    }

    #[test]
    fn zero_rate_is_unlimited() {
        let e = IngestEnforcer::new();
        let l = Limits {
            ingestion_rate: Frequency::ZERO,
            ingestion_burst_size: 0,
            ..Limits::default()
        };
        assert!(e.check_sample_rate(&l, "t", 1_000_000).is_ok());
    }

    #[test]
    fn non_finite_rate_is_handled() {
        let e = IngestEnforcer::new();
        // NaN must not slip past `== 0.0` into the unlimited path, and is
        // treated as "limiting disabled" rather than reaching the int bucket.
        let nan = Limits {
            ingestion_rate: Frequency::from_per_sec(f64::NAN),
            ingestion_burst_size: 0,
            ..Limits::default()
        };
        assert!(e.check_sample_rate(&nan, "nan", 1_000_000).is_ok());
        // +Inf is unbounded throughput, also disabled rather than int-bucketed.
        let inf = Limits {
            ingestion_rate: Frequency::from_per_sec(f64::INFINITY),
            ingestion_burst_size: 0,
            ..Limits::default()
        };
        assert!(e.check_sample_rate(&inf, "inf", 1_000_000).is_ok());
    }

    #[test]
    fn rate_bucket_map_stays_bounded() {
        // Many distinct tenants must not grow the bucket map without bound;
        // LRU eviction keeps it at or below the configured cap.
        let cap = 8;
        let e = IngestEnforcer::with_max_rate_buckets(cap);
        let l = Limits {
            ingestion_rate: per_sec(100),
            ingestion_burst_size: 100,
            ..Limits::default()
        };
        for i in 0..1_000 {
            let tenant = format!("tenant-{i}");
            let _ = e.check_sample_rate(&l, &tenant, 1);
            assert!(e.sample_rate_buckets.len() <= cap);
        }
        assert!(e.sample_rate_buckets.len() <= cap);
    }

    #[test]
    fn default_rate_bucket_cap_is_preserved() {
        check!(DEFAULT_MAX_RATE_BUCKETS == 100_000);
    }

    #[test]
    fn ingestion_burst_is_independent_of_rate() {
        let e = IngestEnforcer::new();
        let l = Limits {
            ingestion_rate: per_sec(100),
            ingestion_burst_size: 1000,
            ..Limits::default()
        };
        check!(e.check_sample_rate(&l, "t", 500).is_ok());
        check!(e.check_sample_rate(&l, "t", 500).is_ok());
        check!(e.check_sample_rate(&l, "t", 1).is_err());
    }

    #[test]
    fn query_range_and_lookback_caps() {
        let l = Limits {
            max_query_length: hours(1),
            max_query_lookback: days(1),
            ..Limits::default()
        };
        let now = 1_000_000_000_000_i64;
        assert!(matches!(
            QueryEnforcer::check_range(&l, now - 7_200_000, now, now),
            Err(LimitError::QueryRangeTooLong { .. })
        ));
        assert!(matches!(
            QueryEnforcer::check_range(&l, now - 172_800_000, now - 172_799_000, now),
            Err(LimitError::QueryLookbackExceeded { .. })
        ));
    }

    #[test]
    fn query_count_caps() {
        let l = Limits {
            max_fetched_series_per_query: 10,
            max_samples_per_query: 1000,
            ..Limits::default()
        };
        check!(QueryEnforcer::check_series_count(&l, 11).is_err());
        check!(QueryEnforcer::check_sample_count(&l, 1001).is_err());
        check!(QueryEnforcer::check_series_count(&l, 10).is_ok());
        // Exactly at the cap is within it, which is what separates `>` from
        // `>=`: read the other way, a tenant is refused the last sample the
        // limit allows them.
        check!(QueryEnforcer::check_sample_count(&l, 1000).is_ok());
    }

    /// A label exactly at the length cap is allowed; one byte over is not.
    ///
    /// Every one of these limits is a `>` against the configured maximum, and
    /// away from the boundary `>` and `>=` agree. Read as `>=`, the cap becomes
    /// one byte tighter than configured and a tenant is refused a label the
    /// documented limit permits.
    #[test]
    fn label_lengths_are_capped_at_the_limit_not_below_it() {
        let l = Limits {
            max_label_name_length: bytes(8),
            max_label_value_length: bytes(4),
            ..Limits::default()
        };
        let labels = |name: &str, value: &str| {
            let mut out = Labels::new();
            out.insert(name, value);
            out
        };

        check!(IngestEnforcer::check_labels(&l, &labels("12345678", "abcd")).is_ok());
        check!(IngestEnforcer::check_labels(&l, &labels("123456789", "abcd")).is_err());
        check!(IngestEnforcer::check_labels(&l, &labels("12345678", "abcde")).is_err());
    }

    /// The query range and lookback caps, at the boundary.
    #[test]
    fn query_extents_are_capped_at_the_limit_not_below_it() {
        let l = Limits {
            max_query_length: secs(60),
            max_query_lookback: secs(600),
            ..Limits::default()
        };
        let now = 1_000_000_000_i64;

        // A range exactly 60s long, ending now: both caps are met exactly.
        check!(QueryEnforcer::check_range(&l, now - 60_000, now, now).is_ok());
        // One millisecond longer than the range cap.
        check!(QueryEnforcer::check_range(&l, now - 60_001, now, now).is_err());
        // Exactly at the lookback cap, then one millisecond past it.
        check!(QueryEnforcer::check_range(&l, now - 600_000, now - 599_000, now).is_ok());
        check!(QueryEnforcer::check_range(&l, now - 600_001, now - 599_000, now).is_err());
    }
}

// === split-modules: generated submodules ===
mod default_max_rate_buckets;
mod extent_between;
mod ingest_enforcer;
mod query_enforcer;
mod rate_bucket;
mod secs_ceil;

pub use default_max_rate_buckets::DEFAULT_MAX_RATE_BUCKETS;
use extent_between::extent_between;
pub use ingest_enforcer::IngestEnforcer;
pub use query_enforcer::QueryEnforcer;
use rate_bucket::RateBucket;
use secs_ceil::secs_ceil;
