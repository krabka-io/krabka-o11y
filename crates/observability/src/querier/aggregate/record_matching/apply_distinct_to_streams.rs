use std::collections::HashSet;

use super::{BTreeMap, Labels, LokiStreamEntry, StreamQuery};

#[derive(Eq, Hash, PartialEq)]
struct DistinctSelection {
    source: Vec<krabka_blockstore::LabelPair>,
    selected: Vec<Option<String>>,
}

pub(crate) fn apply_distinct_to_streams(
    streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>,
    query: &StreamQuery,
) {
    let Some(labels) = query.pipeline.iter().find_map(|stage| match stage {
        krabka_logql::PipelineStage::Distinct(labels) => Some(labels),
        _ => None,
    }) else {
        return;
    };
    let mut seen = HashSet::new();
    streams.retain(|stream_labels, entries| {
        entries.retain(|entry| {
            let source = entry
                .source_labels
                .iter()
                .map(|pair| krabka_blockstore::LabelPair::new(pair.0, pair.1))
                .collect::<Vec<_>>();
            let selected = labels
                .iter()
                .map(|label| stream_labels.get(label).cloned())
                .collect::<Vec<_>>();
            seen.insert(DistinctSelection { source, selected })
        });
        let _ = stream_labels;
        true
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_spans_parsed_output_buckets_but_not_source_streams() {
        let query = StreamQuery {
            matchers: Vec::new(),
            pipeline: vec![krabka_logql::PipelineStage::Distinct(vec!["method".into()])],
        };
        let mut streams = BTreeMap::new();
        struct StreamCase {
            app: &'static str,
            status: &'static str,
        }
        for case in [
            StreamCase {
                app: "api",
                status: "200",
            },
            StreamCase {
                app: "api",
                status: "500",
            },
            StreamCase {
                app: "web",
                status: "200",
            },
        ] {
            let mut labels = Labels::new();
            labels.insert("app".into(), case.app.into());
            labels.insert("method".into(), "GET".into());
            labels.insert("status".into(), case.status.into());
            let mut entry = LokiStreamEntry::new(1, "line".into(), Labels::new(), Labels::new());
            entry.source_labels.insert("app".into(), case.app.into());
            streams.entry(labels).or_insert_with(Vec::new).push(entry);
        }

        apply_distinct_to_streams(&mut streams, &query);

        assert_eq!(streams.values().map(Vec::len).sum::<usize>(), 2);
    }
}
