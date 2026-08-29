    /// The shard-range cache answers only when its entry is both fresh and
    /// covers far enough back, and it *evicts* on either failure rather than
    /// leaving the entry to be retried. Both halves matter: a caller that gets
    /// None refetches, and an entry left behind would be rejected again on
    /// every subsequent call while still occupying the map.
    #[test]
    fn a_stale_or_short_shard_range_entry_is_evicted_not_reused() {
        use std::time::{Duration, Instant};

        let key = super::DynamicShardRangesCacheKey {
            tenant: "t".to_string(),
        };
        let ranges = vec![super::TimeRange {
            start_ns: 100,
            end_ns: 200,
        }];

        let seed = |loaded_at: Instant, listed_from_ns: i64| {
            let cache = super::DynamicIndexCache::default();
            cache.shard_ranges.lock().expect("fresh lock").insert(
                key.clone(),
                super::CachedShardRanges {
                    loaded_at,
                    listed_from_ns,
                    ranges: ranges.clone(),
                },
            );
            cache
        };
        let entries =
            |cache: &super::DynamicIndexCache| cache.shard_ranges.lock().expect("fresh lock").len();

        // Fresh, and covering back to 100: a request from 100 or later is served.
        let cache = seed(Instant::now(), 100);
        check!(
            cache.get_shard_ranges(&key, 100) == Some(ranges.clone()),
            "exactly covered"
        );
        check!(
            cache.get_shard_ranges(&key, 150) == Some(ranges.clone()),
            "more than covered"
        );
        check!(entries(&cache) == 1, "a usable entry stays");

        // Asked for earlier than the entry was listed from: not usable, and
        // dropped so the next call refetches rather than re-rejecting.
        let cache = seed(Instant::now(), 100);
        check!(
            cache.get_shard_ranges(&key, 99) == None,
            "one nanosecond short"
        );
        check!(entries(&cache) == 0, "and evicted");

        // Older than the five-second default TTL.
        let stale = Instant::now()
            .checked_sub(Duration::from_mins(1))
            .expect("an instant a minute ago");
        let cache = seed(stale, 100);
        check!(cache.get_shard_ranges(&key, 100) == None, "expired");
        check!(entries(&cache) == 0, "and evicted");

        // A key that was never cached is simply absent, and nothing is
        // inserted by asking for it.
        let cache = super::DynamicIndexCache::default();
        check!(cache.get_shard_ranges(&key, 100) == None);
        check!(entries(&cache) == 0);
    }

    /// `post_query_params` merges a URL query with a form body. It has a
    /// near-twin, `post_query_params_body_first`, which differs only in which
    /// side leads the result -- so every case here asserts the order, not just
    /// the contents. A test that checked membership alone would pass against
    /// either function and distinguish neither.
    #[test]
    fn a_posted_query_puts_the_url_first_and_the_body_second() {
        let merge = |raw: Option<&str>, body: &str| {
            super::post_query_params(raw, &Bytes::from(body.to_owned())).expect("valid body")
        };
        let body_first = |raw: Option<&str>, body: &str| {
            super::post_query_params_body_first(raw, &Bytes::from(body.to_owned()))
                .expect("valid body")
        };

        // Both sides present: the order is the whole difference between the
        // two functions.
        check!(merge(Some("a=1"), "b=2") == "a=1&b=2");
        check!(body_first(Some("a=1"), "b=2") == "b=2&a=1");

        // One side only, where the two agree.
        check!(merge(Some("a=1"), "") == "a=1");
        check!(merge(None, "b=2") == "b=2");
        check!(merge(None, "") == "");

        // An empty URL query is treated as absent rather than concatenated,
        // which would otherwise leave a leading separator.
        check!(merge(Some(""), "b=2") == "b=2");
        check!(merge(Some(""), "") == "");
        check!(body_first(Some(""), "b=2") == "b=2");
    }

    /// The rules filters read a `Prometheus`-shaped query. Each recognised key
    /// is guarded on its value, so a key carrying something unexpected leaves
    /// the filter unset rather than setting it to a default.
    #[test]
    fn rules_filters_take_only_the_values_they_recognise() {
        use super::PrometheusRulesFilters as Filters;
        let parse = |q: &str| Filters::parse(Some(q)).expect("valid query");

        // `type` maps two spellings and rejects the rest.
        check!(parse("type=alert").rule_kind == Some("alerting"));
        check!(parse("type=record").rule_kind == Some("recording"));
        check!(
            parse("type=other").rule_kind == None,
            "an unknown type sets nothing"
        );
        check!(parse("type=").rule_kind == None);
        check!(
            parse("type=alerting").rule_kind == None,
            "the output spelling is not the input"
        );

        // `exclude_alerts` is only true for the exact string.
        check!(parse("exclude_alerts=true").exclude_alerts);
        check!(!parse("exclude_alerts=false").exclude_alerts);
        check!(
            !parse("exclude_alerts=1").exclude_alerts,
            "only `true` counts"
        );
        check!(
            !parse("exclude_alerts=TRUE").exclude_alerts,
            "case-sensitively"
        );
        check!(!Filters::parse(None).expect("no query").exclude_alerts);

        // The repeated keys accept both spellings and collect rather than
        // replace, and an empty value is skipped rather than collected.
        let names = parse("rule_name=a&rule_name[]=b&rule_name=").rule_names;
        check!(names.len() == 2, "got {names:?}");
        check!(names.contains("a") && names.contains("b"));

        let groups = parse("rule_group=g1&rule_group[]=g2").rule_groups;
        check!(groups.len() == 2);

        // No query at all is a default set of filters, not an error.
        let empty = Filters::parse(None).expect("no query");
        check!(empty.rule_kind == None);
        check!(empty.rule_names.is_empty());
    }

    /// The comparison operators are a six-entry table, and every strict one
    /// has a non-strict twin one character longer. Checking them entry by
    /// entry is what separates the pairs; sampling would not.
    #[test]
    fn every_metric_comparison_operator_maps_to_its_own_variant() {
        use super::{ComparisonOp, parse_metric_comparison_operator as parse};

        check!(parse("==") == Some(ComparisonOp::Equal));
        check!(parse("!=") == Some(ComparisonOp::NotEqual));
        check!(parse(">") == Some(ComparisonOp::Greater));
        check!(parse(">=") == Some(ComparisonOp::GreaterEqual));
        check!(parse("<") == Some(ComparisonOp::Less));
        check!(parse("<=") == Some(ComparisonOp::LessEqual));

        // Nothing else is an operator, including the near-misses.
        check!(parse("") == None);
        check!(parse("=") == None, "a single equals is not a comparison");
        check!(parse("=>") == None);
        check!(parse("=<") == None);
        check!(parse("<>") == None);
        check!(parse("===") == None);
        check!(parse(">>") == None);
    }

    /// `parse_vector_group_modifier` returns the modifier it read and how many
    /// bytes it consumed. The length is the half that matters: a caller
    /// resumes from it, so an off-by-one there re-reads a character or skips
    /// one, and the returned text alone would look correct either way.
    #[test]
    fn a_vector_group_modifier_reports_what_it_consumed() {
        let parse = super::parse_vector_group_modifier;

        // Bare, with the length being the whole modifier.
        check!(parse("group_left", 0) == Some(("group_left".to_string(), 10)));
        check!(parse("group_right", 0) == Some(("group_right".to_string(), 11)));

        // With labels, the length covers the parentheses too.
        check!(parse("group_left(a)", 0) == Some(("group_left (a)".to_string(), 13)));
        check!(parse("group_right(a,b)", 0) == Some(("group_right (a,b)".to_string(), 16)));

        // Empty parentheses are consumed but add no labels.
        check!(parse("group_left()", 0) == Some(("group_left".to_string(), 12)));

        // The length is relative to the whole query, not to the slice read.
        check!(parse("x group_left", 2) == Some(("group_left".to_string(), 12)));
        check!(parse("x group_left(a)", 2) == Some(("group_left (a)".to_string(), 15)));

        // Trailing input is left for the caller rather than swallowed.
        check!(parse("group_left(a) foo", 0) == Some(("group_left (a)".to_string(), 13)));

        // An unclosed parenthesis is not a modifier at all.
        check!(parse("group_left(a", 0) == None);
        check!(parse("nothing", 0) == None);
        check!(parse("", 0) == None);
    }

    /// `MetricValue` is a rational scaled by a fixed decimal factor, so a
    /// float arriving from a metric has to survive the round trip through that
    /// scale, and the values it cannot represent have to be refused rather
    /// than rounded into something plausible.
    #[test]
    fn metric_values_round_trip_through_their_decimal_scale() {
        use super::MetricValue;

        let round_trip =
            |value: f64| MetricValue::from_f64(value).and_then(super::MetricValue::to_f64);

        check!(round_trip(0.0) == Some(0.0));
        check!(round_trip(1.0) == Some(1.0));
        check!(round_trip(-1.0) == Some(-1.0));
        check!(round_trip(0.5) == Some(0.5));
        check!(round_trip(-2.25) == Some(-2.25));
        check!(round_trip(1234.5) == Some(1234.5));

        // The scale is a billion, so a nanosecond-sized fraction survives and
        // anything finer rounds to the nearest step rather than to zero.
        check!(round_trip(0.000_000_001) == Some(0.000_000_001));
        check!(
            round_trip(0.000_000_000_4) == Some(0.0),
            "below half a step rounds down"
        );
        check!(
            round_trip(0.000_000_000_6) == Some(0.000_000_001),
            "above half rounds up"
        );

        // Values that are not numbers cannot be represented at all.
        check!(MetricValue::from_f64(f64::NAN) == None);
        check!(MetricValue::from_f64(f64::INFINITY) == None);
        check!(MetricValue::from_f64(f64::NEG_INFINITY) == None);
    }

    /// `MetricValue::modulo` refuses a zero divisor rather than producing a
    /// NaN, which is the whole reason it is not just `%`.
    #[test]
    fn metric_modulo_refuses_a_zero_divisor() {
        use super::MetricValue;

        let modulo = |a: f64, b: f64| {
            MetricValue::from_f64(a)?
                .modulo(MetricValue::from_f64(b)?)
                .and_then(super::MetricValue::to_f64)
        };

        check!(modulo(7.0, 3.0) == Some(1.0));
        check!(modulo(7.5, 2.5) == Some(0.0));
        check!(
            modulo(-7.0, 3.0) == Some(-1.0),
            "the sign follows the dividend"
        );
        check!(
            modulo(3.0, 7.0) == Some(3.0),
            "a smaller dividend is itself"
        );
        check!(modulo(1.0, 0.0) == None, "a zero divisor has no answer");
        check!(modulo(0.0, 3.0) == Some(0.0), "but a zero dividend does");
    }

    /// `has_samples` gates every aggregate that would otherwise divide by a
    /// count of zero, so it must be false at zero and true at one.
    #[test]
    fn a_sample_state_has_samples_from_the_first_one() {
        let mut state = super::MetricSampleState::default();
        check!(!state.has_samples(), "an empty state has none");

        state.count = 1;
        check!(state.has_samples(), "one sample is enough");
        state.count = 100;
        check!(state.has_samples());
    }

    /// Recording keeps the earliest sample and the latest, and a later record
    /// at a timestamp already held changes neither. The four below arrive out
    /// of order and revisit both ends: without the revisits, the guards could
    /// take the last writer at each end instead of the first.
    #[test]
    fn recording_samples_keeps_the_earliest_and_the_latest() {
        let value = |numerator: i128| super::MetricValue {
            numerator,
            denominator: 1,
        };
        let mut state = super::MetricSampleState::default();

        state.record(10, value(1));
        state.record(5, value(2));
        // Neither of these displaces an end: one repeats the latest timestamp,
        // the other the earliest.
        state.record(10, value(3));
        state.record(5, value(4));

        check!(state.count == 4);
        check!(
            state.first == Some((5, value(2))),
            "the earliest timestamp, from the first record that reached it"
        );
        check!(
            state.last == Some((10, value(1))),
            "the latest timestamp, from the first record that reached it"
        );
    }

    /// A scalar renders its sign from the numerator alone, and a decimal that
    /// does not terminate stops at nine digits. Zero is not negative, which is
    /// what separates `< 0` from `<= 0`; a negative that is not zero separates
    /// it from `== 0`; and a repeating decimal is the only input that reaches
    /// the digit cap at all.
    #[test]
    fn a_scalar_sample_formats_its_sign_and_stops_at_nine_decimals() {
        let format = |numerator: i128, denominator: u128| {
            super::ScalarSample::new(numerator, denominator).format()
        };

        check!(format(0, 1) == "0", "zero carries no sign");
        check!(format(7, 1) == "7");
        check!(format(-7, 1) == "-7");
        check!(format(3, 2) == "1.5");
        check!(format(-3, 2) == "-1.5");
        check!(format(1, 8) == "0.125");

        // Truncated at nine digits, not rounded and not run on.
        check!(format(1, 3) == "0.333333333");
        check!(format(-2, 3) == "-0.666666666");
    }

    /// Merging two partial sample states keeps the smaller minimum, the larger
    /// maximum, the earliest first and the latest last, taking each from
    /// whichever side holds it. A tie on the timestamp keeps the side already
    /// held -- the only thing that separates `<` from `<=` at either end.
    #[test]
    fn merging_sample_states_keeps_the_extremes_and_the_ends() {
        let value = |numerator: i128| super::MetricValue {
            numerator,
            denominator: 1,
        };

        let mut left = super::MetricSampleState {
            count: 1,
            min: Some(value(5)),
            max: Some(value(5)),
            first: Some((10, value(1))),
            last: Some((10, value(1))),
            ..Default::default()
        };
        // Every field of the incoming state wins: a lower minimum, a higher
        // maximum, an earlier first and a later last.
        left.merge(super::MetricSampleState {
            count: 1,
            min: Some(value(3)),
            max: Some(value(9)),
            first: Some((5, value(2))),
            last: Some((20, value(3))),
            ..Default::default()
        });

        check!(left.count == 2);
        check!(left.min == Some(value(3)), "the smaller minimum wins");
        check!(left.max == Some(value(9)), "the larger maximum wins");
        check!(left.first == Some((5, value(2))), "the earlier first wins");
        check!(left.last == Some((20, value(3))), "the later last wins");

        // Now the other way round, so neither side is simply preferred.
        let mut right = super::MetricSampleState {
            count: 1,
            min: Some(value(3)),
            max: Some(value(9)),
            first: Some((5, value(2))),
            last: Some((20, value(3))),
            ..Default::default()
        };
        right.merge(super::MetricSampleState {
            count: 1,
            min: Some(value(5)),
            max: Some(value(5)),
            first: Some((10, value(1))),
            last: Some((10, value(1))),
            ..Default::default()
        });
        check!(right.min == Some(value(3)), "the held minimum survives");
        check!(right.max == Some(value(9)), "the held maximum survives");
        check!(
            right.first == Some((5, value(2))),
            "the held first survives"
        );
        check!(right.last == Some((20, value(3))), "the held last survives");

        // Matching timestamps on both sides: the value already held stays.
        let mut tied = super::MetricSampleState {
            count: 1,
            first: Some((10, value(1))),
            last: Some((10, value(1))),
            ..Default::default()
        };
        tied.merge(super::MetricSampleState {
            count: 1,
            first: Some((10, value(7))),
            last: Some((10, value(7))),
            ..Default::default()
        });
        check!(
            tied.first == Some((10, value(1))),
            "a tie keeps the first already held"
        );
        check!(
            tied.last == Some((10, value(1))),
            "a tie keeps the last already held"
        );
    }

    /// Every rules filter that takes a value ignores an empty one. Without
    /// that guard `time=`, `group_limit=` and `match=` are handed the empty
    /// string to parse and the whole request fails, while `rule_group=`,
    /// `file=` and `group_next_token=` quietly filter on "" and match nothing.
    /// A query naming all of them with no values is indistinguishable from no
    /// query at all.
    #[test]
    fn empty_prometheus_rules_filter_values_are_ignored() {
        let filters = super::PrometheusRulesFilters::parse(Some(
            "time=&rule_name=&rule_group=&file=&group_limit=&group_next_token=&match=",
        ))
        .expect("empty values are ignored, not rejected");

        check!(filters == super::PrometheusRulesFilters::default());
    }

