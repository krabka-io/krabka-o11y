//! A populated in-memory metric store, for `PromQL` range evaluation, and
//! the WAL records that a hot head ingests into one.

use krabka_metrics::{SamplePayload, WalRecord};
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

/// `count` WAL records, one float sample each, over `count` distinct series.
///
/// A poll of the metrics WAL carries a scrape's worth of different series, not
/// one series repeated, and a batch that touched a single label set would let
/// the store's per-series bookkeeping look cheaper than it is. The timestamps
/// sit past the end of a [`metric_store`] fixture so an applied batch adds rows
/// rather than landing inside the range a query already covers.
///
/// # Panics
/// Panics when a record index does not fit an `i64`.
#[must_use]
pub fn wal_records(count: usize) -> Vec<WalRecord> {
    (0..count)
        .map(|which| WalRecord {
            tenant: TENANT.to_string(),
            labels: series_labels(which)
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            payload: SamplePayload::Float {
                timestamp_ms: FIRST_APPLIED_MS
                    + i64::try_from(which).expect("a record index fits an i64"),
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        })
        .collect()
}

/// The timestamp the first record of [`wal_records`] carries, past the end of
/// any [`metric_store`] this suite builds.
const FIRST_APPLIED_MS: i64 = 1_000_000_000;
