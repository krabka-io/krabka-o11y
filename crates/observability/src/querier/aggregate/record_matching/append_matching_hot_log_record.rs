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

    if let Some(matched) = matching_loki_stream_entry(
        &plan.query,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
        preserve_source_labels,
    ) {
        streams
            .entry(matched.labels)
            .or_default()
            .push(matched.entry);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::assert;

    use super::*;

    struct QuerySourceLabelCase {
        query: &'static str,
        preserve_source_labels: bool,
        source_labels: Labels,
    }

    fn app_labels() -> Labels {
        let mut labels = Labels::new();
        labels.insert("app".into(), "api".into());
        labels
    }

    #[test]
    fn ordinary_queries_omit_source_copies_but_distinct_and_tail_keep_them() {
        let labels = app_labels();
        let record = WalLogRecord {
            tenant: "tenant".into(),
            labels: labels.clone(),
            timestamp_ns: 10,
            line: "line".into(),
            structured_metadata: Labels::new(),
            position: None,
        };
        for case in [
            QuerySourceLabelCase {
                query: r#"{app="api"}"#,
                preserve_source_labels: false,
                source_labels: Labels::new(),
            },
            QuerySourceLabelCase {
                query: r#"{app="api"}"#,
                preserve_source_labels: true,
                source_labels: labels.clone(),
            },
            QuerySourceLabelCase {
                query: r#"{app="api"} | distinct app"#,
                preserve_source_labels: false,
                source_labels: labels.clone(),
            },
        ] {
            let plan = StreamPlan {
                tenant: "tenant".into(),
                time_range: krabka_blockstore::TimeRange::new(0, 100).unwrap(),
                query: krabka_logql::parse_query(case.query).unwrap(),
                fingerprints: BTreeSet::default(),
                blocks: Vec::new(),
            };
            let mut actual = BTreeMap::new();
            append_matching_hot_log_record(
                &mut actual,
                &plan,
                &record,
                &CompactionFrontier::new(0),
                &[],
                case.preserve_source_labels,
            );
            let mut expected = BTreeMap::new();
            expected.insert(
                app_labels(),
                vec![LokiStreamEntry {
                    timestamp_ns: "10".into(),
                    line: "line".into(),
                    source_labels: case.source_labels,
                    structured_metadata: Labels::new(),
                    parsed: Labels::new(),
                }],
            );
            assert!(actual == expected);
        }
    }
}
