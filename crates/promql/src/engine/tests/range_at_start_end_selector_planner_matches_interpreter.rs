use super::*;

#[derive(bon::Builder)]
struct SamplePoint {
    timestamp_ms: i64,
    value: f64,
}

struct JobSamples {
    job: &'static str,
    samples: Vec<SamplePoint>,
}

struct RangeQueryBounds {
    start: i64,
    end: i64,
    step: krabka_units::prelude::Time,
}

struct QueryExpectation {
    query: &'static str,
    expect_series: bool,
}

/// Differential parity for a bare top-level selector with `@ start()` or `@ end()`.
///
/// The query is a range query. The per-step planner range driver scopes the
/// query bounds `[start, end]`, and `plan_instant_selector` resolves
/// `@ start()` and `@ end()` to those bounds. The result is a fixed eval
/// instant repeated across every step, the same as the interpreter's dedicated
/// `eval_vector_selector_over_steps`. This proves:
///   1. the gate routes `m @ start()` / `m @ end()` through the planner, and
///   2. planner (public range path) == interpreter byte-for-byte.
#[tokio::test]
pub(crate) async fn range_at_start_end_selector_planner_matches_interpreter() {
    use crate::DurationExprContext;

    let mut store = InMemoryMetricStore::new();
    let jobs = [
        JobSamples {
            job: "a",
            samples: vec![
                SamplePoint::builder().timestamp_ms(0).value(1.0).build(),
                SamplePoint::builder()
                    .timestamp_ms(120_000)
                    .value(2.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(300_000)
                    .value(3.0)
                    .build(),
            ],
        },
        JobSamples {
            job: "b",
            samples: vec![
                SamplePoint::builder().timestamp_ms(0).value(10.0).build(),
                SamplePoint::builder()
                    .timestamp_ms(180_000)
                    .value(20.0)
                    .build(),
                SamplePoint::builder()
                    .timestamp_ms(300_000)
                    .value(30.0)
                    .build(),
            ],
        },
    ];
    for job_index in 0..jobs.len() {
        let job = &jobs[job_index];
        for sample_index in 0..job.samples.len() {
            let sample = &job.samples[sample_index];
            store.push_float(
                "t",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "m"),
                    crate::test_support::TestLabelPair::new("job", job.job),
                ]),
                sample.timestamp_ms,
                sample.value,
            );
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let bounds = RangeQueryBounds {
        start: 0,
        end: 300_000,
        step: millis(60_000),
    };

    // `m @ start()` pins eval to t=0 and `m @ end()` to t=300000: both have
    // an in-window sample, so they yield series. `m @ start() offset 1m`
    // shifts the pinned eval back 60s to t=-60000, whose 5m window
    // (-360000, -60000] holds NO sample (the earliest is at t=0), so it
    // yields an EMPTY matrix — the Prometheus-correct result.
    let queries = [
        QueryExpectation {
            query: "m @ start()",
            expect_series: true,
        },
        QueryExpectation {
            query: "m @ end()",
            expect_series: true,
        },
        QueryExpectation {
            query: "m @ start() offset 1m",
            expect_series: false,
        },
    ];
    for query_index in 0..queries.len() {
        let query = &queries[query_index];
        // (1) the gate routes the `@ start()/end()` selector through the planner.
        assert2::assert!(range_query_routes_through_planner(
            query.query,
            DurationExprContext::range(bounds.start, bounds.end, bounds.step)
        ));

        // (2) the planner resolves `@ start()`/`@ end()` to a FIXED eval
        // instant repeated across every grid step, so each surviving series
        // carries the SAME value at every one of the 6 steps (the value it
        // had at the pinned eval instant), matching Prometheus.
        let QueryResult::RangeMatrix(series) = engine
            .query_range(
                &tenant_id("t"),
                query.query,
                bounds.start,
                bounds.end,
                bounds.step,
            )
            .await
            .unwrap_or_else(|error| panic!("planner `{}`: {error}", query.query))
        else {
            panic!("expected matrix for `{}`", query.query);
        };
        assert2::assert!(!series.is_empty() == query.expect_series);
        for s in &series {
            let times: Vec<i64> = s.samples.iter().map(|sample| sample.timestamp_ms).collect();
            assert2::assert!(times == vec![0, 60_000, 120_000, 180_000, 240_000, 300_000]);
            let values: Vec<u64> = s
                .samples
                .iter()
                .map(|sample| float_value(&sample.value).to_bits())
                .collect();
            assert2::assert!(values.windows(2).all(|w| w[0] == w[1]));
        }
    }

    // Instant-query start/end both mean the query evaluation timestamp.
    for query in ["m @ start()", "m @ end()"] {
        let QueryResult::InstantVector(samples) = engine
            .query_instant(&tenant_id("t"), query, 120_000)
            .await
            .unwrap_or_else(|error| panic!("instant `{query}`: {error}"))
        else {
            panic!("expected instant vector for `{query}`");
        };
        assert2::assert!(samples.len() == 2);
        assert2::assert!(samples.iter().all(|sample| sample.ts_ms == 120_000));
    }
}
