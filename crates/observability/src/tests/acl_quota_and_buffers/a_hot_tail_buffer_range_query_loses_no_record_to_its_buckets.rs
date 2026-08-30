use super::*;

/// The buffer answers a range query through its bucket index, and nothing
/// had exercised that path. The index is a granularity, not a filter: the
/// exact bound is applied within the buckets it scans, so what has to hold
/// is that no record in the window is left behind in a bucket the scan
/// skipped.
#[test]
pub(crate) fn a_hot_tail_buffer_range_query_loses_no_record_to_its_buckets() {
    let minute = minutes(1).nanos_i64();
    let record = |timestamp_ns: i64| WalLogRecord {
        tenant: "t".to_string(),
        labels: Labels::default(),
        timestamp_ns,
        line: timestamp_ns.to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    };

    let tail = super::super::prelude::BufferedLogHotTail::with_bucket_width(minutes(1));
    tail.append_records(vec![record(0), record(minute), record(minute * 2)]);

    let stamps = |start: i64, end: i64| {
        tail.records_in_range(start, end)
            .into_iter()
            .map(|record| record.timestamp_ns)
            .collect::<Vec<_>>()
    };
    check!(tail.records().len() == 3, "every record is kept");
    check!(
        stamps(0, minute) == vec![0, minute],
        "both ends are inclusive"
    );
    check!(
        stamps(1, minute - 1) == Vec::<i64>::new(),
        "a window between two records holds neither"
    );
    check!(
        stamps(0, minute * 2) == vec![0, minute, minute * 2],
        "a window spanning every bucket returns every record"
    );
}
