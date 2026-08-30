use super::*;

/// The time-bucketed `records_in_range` MUST return exactly the same records (and
/// so the same label/field sets) as a full-buffer scan, for any inclusive
/// `[start, end]`, even though records are appended in NO timestamp order. This is the
/// soundness guarantee that lets the query paths prune to the window instead of
/// scanning the whole retained buffer.
#[tokio::test]
pub(crate) async fn hot_tail_records_in_range_matches_full_scan_under_out_of_order_inserts() {
    let bucket = minutes(1).nanos_i64();

    let hot_tail = BufferedLogHotTail::default();

    // Timestamps deliberately out of order and spread across many one-minute buckets,
    // with duplicates at the same instant and records straddling bucket boundaries.
    let timestamps = [
        5 * bucket + 10,
        bucket - 1, // last ns of bucket 0
        3 * bucket,
        bucket,          // first ns of bucket 1
        5 * bucket + 10, // duplicate timestamp
        0,
        7 * bucket + 42,
        2 * bucket + 999,
        3 * bucket, // duplicate timestamp in a different append position
        4 * bucket - 1,
        -bucket + 5, // a pre-epoch (negative) timestamp
        6 * bucket,
    ];
    let apps = ["api", "web", "db"];
    let records: Vec<WalLogRecord> = timestamps
        .iter()
        .enumerate()
        .map(|(i, &ts)| hot_tail_test_record(ts, apps[i % apps.len()]))
        .collect();

    // Append one at a time to exercise incremental bucket insertion of out-of-order data.
    for record in &records {
        hot_tail.append_records(vec![record.clone()]);
    }

    // `records()` must still return the full append-ordered buffer (the tail path
    // depends on this).
    assert_eq!(hot_tail.records(), records);

    // Probe a wide set of windows: exact bucket edges, sub-bucket slivers, windows
    // spanning many buckets, empty windows, and windows entirely outside the data.
    let min_ts = *timestamps.iter().min().unwrap();
    let max_ts = *timestamps.iter().max().unwrap();
    let mut probes: Vec<(i64, i64)> = Vec::new();
    // Walk window starts at a coarse quarter-bucket stride from below the earliest
    // record to above the latest, pairing each with several spans.
    let stride = bucket / 4;
    let mut start = min_ts - 2 * bucket;
    while start <= max_ts + 2 * bucket {
        for span in [0_i64, 1, bucket - 1, bucket, bucket + 1, 3 * bucket] {
            probes.push((start, start + span));
        }
        start += stride;
    }
    // Add exact per-record point windows and tight windows around each timestamp.
    for &ts in &timestamps {
        probes.push((ts, ts));
        probes.push((ts - 1, ts));
        probes.push((ts, ts + 1));
        probes.push((ts + 1, ts + 1));
    }

    for (start, end) in probes {
        if start > end {
            // Mirror the guard: an inverted window yields nothing.
            assert!(hot_tail.records_in_range(start, end).is_empty());
            continue;
        }
        let expected = brute_force_in_range(&records, start, end);
        let actual = hot_tail.records_in_range(start, end);
        assert_eq!(
            actual, expected,
            "records_in_range({start}, {end}) diverged from full-scan oracle"
        );

        // The label sets a query would derive must be identical too (records are the
        // sole input to label/field extraction).
        let expected_labels: BTreeSet<Labels> = expected.iter().map(|r| r.labels.clone()).collect();
        let actual_labels: BTreeSet<Labels> = actual.iter().map(|r| r.labels.clone()).collect();
        assert_eq!(
            actual_labels, expected_labels,
            "label sets diverged at [{start}, {end}]"
        );
    }

    // The trait-object path the querier actually uses must agree with the inherent method.
    let dyn_tail: Arc<dyn LogHotTail> = Arc::new(hot_tail.clone());
    let window = (2 * bucket, 6 * bucket);
    assert_eq!(
        dyn_tail.records_in_range(window.0, window.1),
        hot_tail.records_in_range(window.0, window.1),
    );

    // The default trait impl (used by other LogHotTail implementors, e.g. the
    // in-memory sink) falls back to filtering the full buffer and must also agree.
    let in_memory = InMemoryWalSink::default();
    for record in &records {
        LogWalSink::append(&in_memory, record.clone())
            .await
            .unwrap();
    }
    let in_memory_dyn: Arc<dyn LogHotTail> = Arc::new(in_memory);
    assert_eq!(
        in_memory_dyn.records_in_range(window.0, window.1),
        brute_force_in_range(&records, window.0, window.1),
    );
}
