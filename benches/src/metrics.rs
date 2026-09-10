//! A populated in-memory metric store, for `PromQL` range evaluation.

use krabka_promql::InMemoryMetricStore;

use crate::index::{TENANT, series_labels};

/// The interval between two samples of the same series, in milliseconds.
pub const SCRAPE_INTERVAL_MS: i64 = 15_000;

/// A store holding `series` series with `samples` samples each.
///
/// The samples are a monotonically rising counter rather than noise, because
/// the queries the benchmark runs are `rate` and `sum by`, and a counter that
/// goes backwards makes `rate` spend its time in reset detection -- which is
/// real work, but not the work a range query does on a healthy series, and not
/// a cost that should move when something unrelated changes.
///
/// # Panics
/// Panics when a sample index does not fit an `i64`, which would need a store
/// larger than any machine this runs on.
#[must_use]
pub fn metric_store(series: usize, samples: usize) -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for which in 0..series {
        let labels = series_labels(which);
        let mut counter = 0.0_f64;
        for sample in 0..samples {
            counter += 1.0;
            let ts =
                i64::try_from(sample).expect("a sample index fits an i64") * SCRAPE_INTERVAL_MS;
            store.push_float(TENANT, labels.clone(), ts, counter);
        }
    }
    store
}

/// The last timestamp a store built with `samples` samples per series carries.
///
/// # Panics
/// Panics when a sample index does not fit an `i64`.
#[must_use]
pub fn end_ms(samples: usize) -> i64 {
    i64::try_from(samples.saturating_sub(1)).expect("a sample index fits an i64")
        * SCRAPE_INTERVAL_MS
}
