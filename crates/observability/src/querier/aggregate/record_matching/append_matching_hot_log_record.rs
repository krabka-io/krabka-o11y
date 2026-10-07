use std::collections::{HashMap, hash_map::Entry};

use super::{
    ActiveLogDeleteFilter, BTreeMap, CompactionFrontier, Labels, LokiStreamEntry, StreamPlan,
    WalLogRecord, is_deleted_log_entry, matching_loki_stream_entry,
};

pub(crate) fn append_matching_hot_log_record<'a>(
    streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>,
    plan: &StreamPlan,
    record: &'a WalLogRecord,
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
    preserve_source_labels: bool,
    labels_cache: &mut HashMap<&'a Labels, (Labels, Labels)>,
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

    // These queries change neither the line nor labels between records of
    // one source stream. The caller keeps this cache within one query.
    if plan.query.pipeline.is_empty() && record.structured_metadata.is_empty() {
        let (labels, metadata) = match labels_cache.entry(&record.labels) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let Some((labels, value)) = matching_loki_stream_entry(
                    &plan.query,
                    &record.labels,
                    &record.line,
                    &record.structured_metadata,
                    record.timestamp_ns,
                    false,
                ) else {
                    return;
                };
                entry.insert((labels, value.structured_metadata))
            }
        };
        let mut entry = LokiStreamEntry::new(
            record.timestamp_ns,
            record.line.clone(),
            metadata.clone(),
            Labels::new(),
        );
        if preserve_source_labels {
            entry.source_labels = record.labels.clone();
        }
        if let Some(entries) = streams.get_mut(labels) {
            entries.push(entry);
        } else {
            streams.insert(labels.clone(), vec![entry]);
        }
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
    use std::collections::BTreeSet;

    use assert2::assert;

    use super::*;
    use crate::{Offset, PartitionIndex};

    #[test]
    fn cached_label_buckets_match_uncached_records_and_existing_streams() {
        let mut records = (0..14)
            .map(|index| WalLogRecord {
                tenant: "tenant".into(),
                labels: Labels::from([("app".into(), "api".into())]),
                timestamp_ns: 10 + index,
                line: format!("keep-{index}"),
                structured_metadata: Labels::new(),
                position: None,
            })
            .collect::<Vec<_>>();
        records[0].timestamp_ns = 3;
        records[2].structured_metadata = Labels::from([
            ("trace_id".into(), "trace".into()),
            ("app".into(), "metadata-app".into()),
        ]);
        records[4].labels.insert("app".into(), "db".into());
        records[5].timestamp_ns = 30;
        records[5].line = "drop".into();
        records[6].tenant = "other".into();
        records[7].timestamp_ns = 101;
        records[8].position = Some(crate::WalPosition {
            partition: PartitionIndex(0),
            offset: Offset(4),
        });
        records[9].position = Some(crate::WalPosition {
            partition: PartitionIndex(0),
            offset: Offset(5),
        });
        records[10].labels.insert("level".into(), "info".into());
        records[11].labels = records[10].labels.clone();
        records[12].timestamp_ns = 30;
        records[13]
            .labels
            .insert("detected_level".into(), "warn".into());
        let frontier =
            CompactionFrontier::new(3).with_partition_offset(PartitionIndex(0), Offset(4));
        let deletes = [ActiveLogDeleteFilter {
            time_range: krabka_blockstore::TimeRange::new(30, 30).unwrap(),
            query: krabka_logql::parse_query(r#"{app="api"} |= "drop""#).unwrap(),
        }];

        for query in [
            r#"{app="api"}"#,
            r#"{app="missing"}"#,
            r#"{app=~"api|db"}"#,
            r#"{app="api"} |= "keep""#,
            r#"{app="api"} | json"#,
            r#"{app="api"} | distinct app"#,
        ] {
            let plan = StreamPlan {
                tenant: "tenant".into(),
                time_range: krabka_blockstore::TimeRange::new(5, 100).unwrap(),
                query: krabka_logql::parse_query(query).unwrap(),
                fingerprints: BTreeSet::new(),
                blocks: Vec::new(),
            };
            for preserve in [false, true] {
                let mut expected: BTreeMap<Labels, Vec<LokiStreamEntry>> = BTreeMap::new();
                if let Some((labels, entry)) = matching_loki_stream_entry(
                    &plan.query,
                    &records[1].labels,
                    "cold",
                    &Labels::new(),
                    11,
                    preserve,
                ) {
                    expected.insert(labels, vec![entry]);
                }
                let mut actual = expected.clone();
                let mut cache = HashMap::new();
                for (index, record) in records.iter().enumerate() {
                    // This ledger decides eligibility independently of the
                    // production frontier and deletion predicates.
                    if ![0, 5, 6, 7, 8].contains(&index)
                        && let Some((labels, entry)) = matching_loki_stream_entry(
                            &plan.query,
                            &record.labels,
                            &record.line,
                            &record.structured_metadata,
                            record.timestamp_ns,
                            preserve,
                        )
                    {
                        expected.entry(labels).or_default().push(entry);
                    }
                    append_matching_hot_log_record(
                        &mut actual,
                        &plan,
                        record,
                        &frontier,
                        &deletes,
                        preserve,
                        &mut cache,
                    );
                }
                assert!(actual == expected, "{query}, preserve={preserve}");
                assert!(
                    cache.is_empty()
                        == (!plan.query.pipeline.is_empty() || query == r#"{app="missing"}"#)
                );
            }
        }
    }

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
                preserve,
                &mut HashMap::new(),
            );
            let expected = BTreeMap::from([(
                Labels::from([("app".into(), "api".into())]),
                vec![LokiStreamEntry {
                    timestamp_ns: "10".into(),
                    line: "line".into(),
                    source_labels,
                    structured_metadata: Labels::new(),
                    parsed: Labels::new(),
                }],
            )]);
            assert!(actual == expected);
        }
    }
}
