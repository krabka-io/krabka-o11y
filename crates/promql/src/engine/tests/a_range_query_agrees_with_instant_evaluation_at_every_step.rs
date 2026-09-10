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
    let mut store = InMemoryMetricStore::new();
    for (name, job, samples) in [
        (
            "http_requests_total",
            "api",
            vec![
                (0_i64, 0.0),
                (60_000, 1.0),
                (120_000, 3.0),
                (180_000, 6.0),
                (240_000, 10.0),
                (300_000, 15.0),
            ],
        ),
        (
            "http_requests_total",
            "db",
            vec![
                (0, 0.0),
                (60_000, 2.0),
                (120_000, 4.0),
                // A gap, so some steps have a no-value rate and some series
                // drop out of a step entirely.
                (300_000, 9.0),
            ],
        ),
        (
            "gauge",
            "api",
            vec![
                (0, 2.0),
                (60_000, 4.0),
                (120_000, f64::NAN),
                (180_000, 16.0),
                (240_000, 32.0),
                (300_000, 64.0),
            ],
        ),
    ] {
        for (ts_ms, value) in samples {
            store.push_float(
                "t",
                labels(&[("__name__", name), ("job", job)]),
                ts_ms,
                value,
            );
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let (start_ms, end_ms, step_ms) = (0_i64, 300_000_i64, 60_000_i64);
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
            .query_range("t", query, start_ms, end_ms, step)
            .await
            .unwrap_or_else(|error| panic!("range `{query}`: {error}"))
        else {
            panic!("expected a range matrix for `{query}`");
        };
        let mut got: BTreeMap<String, Vec<(i64, u64)>> = BTreeMap::new();
        for one in &series {
            got.insert(
                labels_key(&one.labels),
                one.samples
                    .iter()
                    .map(|(ts_ms, value)| (*ts_ms, float_value(value).to_bits()))
                    .collect(),
            );
        }

        let mut want: BTreeMap<String, Vec<(i64, u64)>> = BTreeMap::new();
        let mut instant_ms = start_ms;
        while instant_ms <= end_ms {
            let result = engine
                .query_instant("t", query, instant_ms)
                .await
                .unwrap_or_else(|error| panic!("instant `{query}` at {instant_ms}: {error}"));
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected an instant vector for `{query}`");
            };
            for sample in samples {
                want.entry(labels_key(&sample.labels))
                    .or_default()
                    .push((instant_ms, float_value(&sample.value).to_bits()));
            }
            instant_ms += step_ms;
        }

        check!(got == want, "`{query}` diverged between range and instant");
    }
}
