use super::{
    ActiveLogDeleteFilter, BTreeMap, CompactionFrontier, Labels, LokiStreamEntry, StreamPlan,
    WalLogRecord, is_deleted_log_entry, matching_loki_stream_entry,
};

pub(crate) fn append_matching_hot_log_record(
    streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>,
    plan: &StreamPlan,
    record: &WalLogRecord,
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
    preserve_source_labels: bool,
) {
    if record.tenant != plan.tenant
        || frontier.is_compacted(record)
        || record.timestamp_ns < plan.time_range.start_ns
        || record.timestamp_ns > plan.time_range.end_ns
    {
        return;
    }

    if is_deleted_log_entry(
        delete_filters,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    ) {
        return;
    }

    if let Some((stream_labels, entry)) = matching_loki_stream_entry(
        &plan.query,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
        preserve_source_labels,
    ) {
        streams.entry(stream_labels).or_default().push(entry);
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn ordinary_queries_omit_source_copies_but_distinct_and_tail_keep_them() {
        let labels = Labels::from([("app".into(), "api".into())]);
        let record = WalLogRecord {
            tenant: "tenant".into(),
            labels: labels.clone(),
            timestamp_ns: 10,
            line: "line".into(),
            structured_metadata: Labels::new(),
            position: None,
        };
        for (query, preserve, source_labels) in [
            (r#"{app="api"}"#, false, Labels::new()),
            (r#"{app="api"}"#, true, labels.clone()),
            (r#"{app="api"} | distinct app"#, false, labels.clone()),
        ] {
            let plan = StreamPlan {
                tenant: "tenant".into(),
                time_range: krabka_blockstore::TimeRange::new(0, 100).unwrap(),
                query: krabka_logql::parse_query(query).unwrap(),
                fingerprints: Default::default(),
                blocks: Vec::new(),
            };
            let mut actual = BTreeMap::new();
            append_matching_hot_log_record(
                &mut actual,
                &plan,
                &record,
                &CompactionFrontier::new(0),
                &[],
                preserve,
            );
            let expected = BTreeMap::from([(
                Labels::from([
                    ("app".into(), "api".into()),
                    ("detected_level".into(), "unknown".into()),
                ]),
                vec![LokiStreamEntry {
                    timestamp_ns: "10".into(),
                    line: "line".into(),
                    source_labels,
                    structured_metadata: Labels::from([(
                        "detected_level".into(),
                        "unknown".into(),
                    )]),
                    parsed: Labels::new(),
                }],
            )]);
            assert!(actual == expected);
        }
    }
}
