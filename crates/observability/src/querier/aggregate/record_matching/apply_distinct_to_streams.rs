use std::collections::HashSet;

use super::{BTreeMap, Labels, LokiStreamEntry, StreamQuery};

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
    for (stream, entries) in streams {
        entries.retain(|entry| {
            let source = entry
                .source_labels
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect::<Vec<_>>();
            let selected = labels
                .iter()
                .map(|label| stream.get(label).cloned())
                .collect::<Vec<_>>();
            seen.insert((source, selected))
        });
    }
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
        for (app, status) in [("api", "200"), ("api", "500"), ("web", "200")] {
            let labels = Labels::from([
                ("app".into(), app.into()),
                ("method".into(), "GET".into()),
                ("status".into(), status.into()),
            ]);
            let mut entry = LokiStreamEntry::new(1, "line".into(), Labels::new(), Labels::new());
            entry.source_labels = Labels::from([("app".into(), app.into())]);
            streams.entry(labels).or_insert_with(Vec::new).push(entry);
        }

        apply_distinct_to_streams(&mut streams, &query);

        assert_eq!(streams.values().map(Vec::len).sum::<usize>(), 2);
    }
}
