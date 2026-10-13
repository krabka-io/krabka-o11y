use super::{super::labels::labels_key, *};

/// A range query's grid-driven leaves agree with evaluating the query one
/// instant at a time.
///
/// A range query plans each operator leaf once over the whole step grid and
/// serves every step from that one execution. An instant query has no grid to
/// drive, so it plans that single instant. The two must produce the same value
/// for the same series at the same instant, bit for bit, or the hoist has
/// changed what the engine answers.
///
/// The shapes below are the ones the memo covers and the ones that must escape
/// it: a bare selector and a rate (memoized), the same two behind an `offset`
/// (memoized on a shifted grid), an aggregation over a rate and a scalar-math
/// call over a selector (memoized through a nested leaf), an `*_over_time` fold,
/// a `quantile_over_time` whose `phi` moves between steps (memoized once and
/// then given up), a selector pinned by `@` (never memoized), and a subquery,
/// whose sub-steps fall between the outer grid's instants and so must miss the
/// memo.
#[tokio::test]
pub(crate) async fn a_range_query_agrees_with_instant_evaluation_at_every_step() {
    struct StoredSeries<'a> {
        metric_name: &'a str,
        job: &'a str,
        samples: &'a [TimedValue],
    }
    struct TimedValue {
        timestamp_ms: i64,
        value: f64,
    }
    #[derive(PartialEq, Eq)]
    struct RangeSampleBits {
        timestamp_ms: i64,
        value_bits: u64,
    }

    let mut store = InMemoryMetricStore::new();
    let series_fixtures = [
        StoredSeries {
            metric_name: "http_requests_total",
            job: "api",
            samples: &[
                TimedValue {
                    timestamp_ms: 0,
                    value: 0.0,
                },
                TimedValue {
                    timestamp_ms: 60_000,
                    value: 1.0,
                },
                TimedValue {
                    timestamp_ms: 120_000,
                    value: 3.0,
                },
                TimedValue {
                    timestamp_ms: 180_000,
                    value: 6.0,
                },
                TimedValue {
                    timestamp_ms: 240_000,
                    value: 10.0,
                },
                TimedValue {
                    timestamp_ms: 300_000,
                    value: 15.0,
                },
            ],
        },
        StoredSeries {
            metric_name: "http_requests_total",
            job: "db",
            samples: &[
                TimedValue {
                    timestamp_ms: 0,
                    value: 0.0,
                },
                TimedValue {
                    timestamp_ms: 60_000,
                    value: 2.0,
                },
                TimedValue {
                    timestamp_ms: 120_000,
                    value: 4.0,
                },
                // A gap, so some steps have a no-value rate and some series
                // drop out of a step entirely.
                TimedValue {
                    timestamp_ms: 300_000,
                    value: 9.0,
                },
            ],
        },
        StoredSeries {
            metric_name: "gauge",
            job: "api",
            samples: &[
                TimedValue {
                    timestamp_ms: 0,
                    value: 2.0,
                },
                TimedValue {
                    timestamp_ms: 60_000,
                    value: 4.0,
                },
                TimedValue {
                    timestamp_ms: 120_000,
                    value: f64::NAN,
                },
                TimedValue {
                    timestamp_ms: 180_000,
                    value: 16.0,
                },
                TimedValue {
                    timestamp_ms: 240_000,
                    value: 32.0,
                },
                TimedValue {
                    timestamp_ms: 300_000,
                    value: 64.0,
                },
            ],
        },
    ];
    for series in series_fixtures {
        for sample in series.samples {
            store.push_float(
                "t",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", series.metric_name),
                    crate::test_support::TestLabelPair::new("job", series.job),
                ]),
                sample.timestamp_ms,
                sample.value,
            );
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let start_ms = 0_i64;
    let end_ms = 300_000_i64;
    let step_ms = 60_000_i64;
    let step = millis(60_000);
    for query in [
        "gauge",
        "gauge offset 1m",
        "rate(http_requests_total[2m])",
        "rate(http_requests_total[2m] offset 1m)",
        "sum by (job) (rate(http_requests_total[2m]))",
        "count by (job) (rate(http_requests_total[2m]))",
        "abs(gauge - 10)",
        "max_over_time(gauge[3m])",
        // `phi` is resolved per step and this one moves with the data, so the
        // memo must give the leaf up rather than answer a later step from the
        // quantile an earlier one asked for.
        "quantile_over_time(scalar(gauge) / 100, gauge[3m])",
        "gauge @ 120",
        "last_over_time(gauge[3m:1m])",
    ] {
        let QueryResult::RangeMatrix(series) = engine
            .query_range(&tenant_id("t"), query, start_ms, end_ms, step)
            .await
            .unwrap_or_else(|error| panic!("range `{query}`: {error}"))
        else {
            panic!("expected a range matrix for `{query}`");
        };
        let mut got: BTreeMap<String, Vec<RangeSampleBits>> = BTreeMap::new();
        for one in &series {
            got.insert(
                labels_key(&one.labels),
                one.samples
                    .iter()
                    .map(|sample| RangeSampleBits {
                        timestamp_ms: sample.timestamp_ms,
                        value_bits: float_value(&sample.value).to_bits(),
                    })
                    .collect(),
            );
        }

        let mut want: BTreeMap<String, Vec<RangeSampleBits>> = BTreeMap::new();
        let mut instant_ms = start_ms;
        while instant_ms <= end_ms {
            let result = engine
                .query_instant(&tenant_id("t"), query, instant_ms)
                .await
                .unwrap_or_else(|error| panic!("instant `{query}` at {instant_ms}: {error}"));
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected an instant vector for `{query}`");
            };
            for sample in samples {
                want.entry(labels_key(&sample.labels))
                    .or_default()
                    .push(RangeSampleBits {
                        timestamp_ms: instant_ms,
                        value_bits: float_value(&sample.value).to_bits(),
                    });
            }
            instant_ms += step_ms;
        }

        check!(got == want, "`{query}` diverged between range and instant");
    }
}
