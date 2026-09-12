use super::{BTreeMap, Labels, LokiStreamEntry, StreamQuery};
use std::collections::HashSet;

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
    for (stream, entries) in streams {
        let key = labels
            .iter()
            .map(|label| stream.get(label).cloned())
            .collect::<Vec<_>>();
        let mut seen = HashSet::new();
        entries.retain(|_| seen.insert(key.clone()));
    }
}
