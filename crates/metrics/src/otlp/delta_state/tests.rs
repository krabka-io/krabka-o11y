use std::time::Duration;

use assert2::{assert, check};

use super::*;

fn labels(name: &str) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    labels
}

fn histogram() -> NativeHistogram {
    NativeHistogram::builder()
        .count(2.0)
        .zero_count(2.0)
        .sum(3.0)
        .build()
}

fn populate(accumulator: &mut DeltaAccumulator, count: usize, start: Instant) {
    for index in 0..count {
        let key = delta_key(&labels(&format!("stream_{index:06}")));
        // Non-monotonic ages force selection beyond the first map entries.
        let now = start + Duration::from_secs(u64::try_from(index % 7).unwrap());
        if index.is_multiple_of(2) {
            accumulator.sums.insert(key, DeltaState::new(now));
        } else {
            accumulator
                .histograms
                .insert(key, DeltaHistogramState::new(now));
        }
    }
}

// Independent reference for the original repeated-minimum rule. BTreeMap
// iteration breaks ties by key; a histogram wins only when strictly older.
fn evict_reference(accumulator: &mut DeltaAccumulator, cap: u64) {
    if cap == 0 {
        return;
    }
    let cap = usize::try_from(cap).unwrap_or(usize::MAX);
    while accumulator.sums.len() + accumulator.histograms.len() > cap {
        let sum = accumulator
            .sums
            .iter()
            .min_by_key(|(_, state)| state.last_seen);
        let histogram = accumulator
            .histograms
            .iter()
            .min_by_key(|(_, state)| state.last_seen);
        let (kind, key) = match (sum, histogram) {
            (Some((_, sum)), Some((key, histogram))) if histogram.last_seen < sum.last_seen => {
                (StreamKind::Histogram, key.clone())
            }
            (Some((key, _)), _) => (StreamKind::Sum, key.clone()),
            (None, Some((key, _))) => (StreamKind::Histogram, key.clone()),
            (None, None) => break,
        };
        match kind {
            StreamKind::Sum => {
                accumulator.sums.remove(&key);
            }
            StreamKind::Histogram => {
                accumulator.histograms.remove(&key);
            }
        }
    }
}

#[test]
fn bulk_eviction_matches_repeated_minimums() {
    let mut original = DeltaAccumulator::default();
    populate(&mut original, 128, Instant::now());
    // The same label key can exist in both maps.
    let shared = original.sums.first_key_value().unwrap().0.clone();
    original
        .histograms
        .insert(shared, DeltaHistogramState::new(original.now));
    for map in ["mixed", "sums", "histograms", "empty"] {
        for cap in [0, 1, 2, 7, 64, 128, 129, 130, u64::MAX] {
            let mut actual = original.clone();
            match map {
                "sums" => actual.histograms.clear(),
                "histograms" => actual.sums.clear(),
                "empty" => {
                    actual.sums.clear();
                    actual.histograms.clear();
                }
                _ => {}
            }
            let mut expected = actual.clone();
            evict_reference(&mut expected, cap);
            actual.evict_if_over_cap(cap);
            check!(
                actual.sums.keys().eq(expected.sums.keys()),
                "{map}, cap={cap}"
            );
            check!(
                actual.histograms.keys().eq(expected.histograms.keys()),
                "{map}, cap={cap}"
            );
        }
    }
}

#[test]
fn equal_ages_evict_sums_before_histograms_and_then_by_key() {
    let mut original = DeltaAccumulator::default();
    let now = original.now;
    for name in ["z", "a", "m"] {
        original
            .sums
            .insert(delta_key(&labels(name)), DeltaState::new(now));
        original
            .histograms
            .insert(delta_key(&labels(name)), DeltaHistogramState::new(now));
    }
    for (cap, sums, histograms) in [
        (5, vec!["m", "z"], vec!["a", "m", "z"]),
        (3, vec![], vec!["a", "m", "z"]),
        (2, vec![], vec!["m", "z"]),
        (1, vec![], vec!["z"]),
    ] {
        let mut accumulator = original.clone();
        accumulator.evict_if_over_cap(cap);
        let expected = |names: Vec<&str>| {
            names
                .into_iter()
                .map(|name| delta_key(&labels(name)))
                .collect::<Vec<_>>()
        };
        check!(accumulator.sums.keys().cloned().collect::<Vec<_>>() == expected(sums));
        check!(accumulator.histograms.keys().cloned().collect::<Vec<_>>() == expected(histograms));
    }
}

#[test]
fn stale_cleanup_precedes_eviction_and_respects_prune_interval() {
    let mut accumulator = DeltaAccumulator::default();
    let start = accumulator.now;
    accumulator.accumulate_sum(&labels("stale_sum"), 1, 5.0);
    accumulator
        .accumulate_histogram("stale_hist", &labels("stale_hist"), 1, histogram())
        .unwrap();
    accumulator.seen_at(start + Duration::from_secs(300));
    accumulator.accumulate_sum(&labels("live_sum"), 1, 5.0);
    accumulator
        .accumulate_histogram("live_hist", &labels("live_hist"), 1, histogram())
        .unwrap();
    let limits = Limits {
        otlp_delta_max_streams: 2,
        otlp_delta_max_stale: minutes(5),
        ..Limits::default()
    };
    accumulator.bound(&limits);
    check!(
        accumulator
            .sums
            .keys()
            .eq([delta_key(&labels("live_sum"))].iter())
    );
    check!(
        accumulator
            .histograms
            .keys()
            .eq([delta_key(&labels("live_hist"))].iter())
    );
    accumulator.seen_at(start + Duration::from_secs(359));
    let short_stale = Limits {
        otlp_delta_max_stale: krabka_units::secs(30),
        ..limits
    };
    accumulator.bound(&short_stale);
    check!(accumulator.sums.len() + accumulator.histograms.len() == 2);
    accumulator.seen_at(start + Duration::from_secs(360));
    accumulator.bound(&short_stale);
    check!(accumulator.sums.is_empty() && accumulator.histograms.is_empty());
}

#[test]
fn evicted_streams_restart_and_retained_streams_keep_delta_reset_behavior() {
    let mut accumulator = DeltaAccumulator::default();
    let start = accumulator.now;
    for name in ["evict", "keep"] {
        accumulator.accumulate_sum(&labels(name), 1, 5.0);
        accumulator
            .accumulate_histogram(name, &labels(name), 1, histogram())
            .unwrap();
        accumulator.seen_at(start + Duration::from_secs(1));
    }
    accumulator.evict_if_over_cap(2);
    assert!(accumulator.sums.contains_key(&delta_key(&labels("keep"))));
    assert!(
        accumulator
            .histograms
            .contains_key(&delta_key(&labels("keep")))
    );
    check!((accumulator.accumulate_sum(&labels("keep"), 1, 5.0) - 10.0).abs() < f64::EPSILON);
    let kept = accumulator
        .accumulate_histogram("keep", &labels("keep"), 1, histogram())
        .unwrap();
    check!(
        kept == NativeHistogram::builder()
            .count(4.0)
            .zero_count(4.0)
            .sum(6.0)
            .build()
    );
    check!((accumulator.accumulate_sum(&labels("keep"), 2, 5.0) - 5.0).abs() < f64::EPSILON);
    check!(
        accumulator
            .accumulate_histogram("keep", &labels("keep"), 2, histogram())
            .unwrap()
            == histogram()
    );
    check!((accumulator.accumulate_sum(&labels("evict"), 1, 5.0) - 5.0).abs() < f64::EPSILON);
    check!(
        accumulator
            .accumulate_histogram("evict", &labels("evict"), 1, histogram())
            .unwrap()
            == histogram()
    );
}

#[test]
#[ignore = "manual latency comparison with 100,000 preloaded streams"]
fn eviction_latency_under_same_tenant_contention() {
    use std::sync::{Barrier, mpsc};
    type Evict = fn(&mut DeltaAccumulator, u64);
    println!("algorithm,new_streams,eviction_us,concurrent_same_tenant_us");
    for (name, evict) in [
        ("repeated_scan", evict_reference as Evict),
        ("bounded_heap", DeltaAccumulator::evict_if_over_cap as Evict),
    ] {
        for extra in [1, 100, 1_000, 10_000] {
            let mut accumulator = DeltaAccumulator::default();
            let start = accumulator.now;
            populate(&mut accumulator, 100_000, start);
            accumulator.seen_at(start + Duration::from_secs(10));
            // Each point creates a distinct stream, as in a churn request.
            for index in 0..extra {
                accumulator.accumulate_sum(&labels(&format!("new_{index:06}")), 1, 1.0);
            }
            let accumulator = Arc::new(Mutex::new(accumulator));
            let barrier = Arc::new(Barrier::new(2));
            let (ready_tx, ready_rx) = mpsc::channel();
            let mut guard = accumulator.lock().unwrap();
            let waiter = {
                let accumulator = Arc::clone(&accumulator);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    let started = Instant::now();
                    ready_tx.send(()).unwrap();
                    let mut guard = accumulator.lock().unwrap();
                    guard.seen_at(start + Duration::from_secs(11));
                    guard.accumulate_sum(&labels("same_tenant"), 1, 1.0);
                    evict(&mut guard, 100_000);
                    started.elapsed()
                })
            };
            barrier.wait();
            ready_rx.recv().unwrap();
            let started = Instant::now();
            evict(&mut guard, 100_000);
            let eviction = started.elapsed();
            assert!(guard.sums.len() + guard.histograms.len() == 100_000);
            drop(guard);
            let concurrent = waiter.join().unwrap();
            println!(
                "{name},{extra},{:.2},{:.2}",
                eviction.as_secs_f64() * 1e6,
                concurrent.as_secs_f64() * 1e6
            );
        }
    }
}
