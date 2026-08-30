use super::SearchResponse;

pub(crate) fn span_ids(resp: &SearchResponse) -> Vec<u8> {
    let mut ids = resp
        .traces
        .iter()
        .flat_map(|trace| trace.span_sets.iter())
        .flat_map(|set| set.spans.iter())
        .map(|span| span.span_id[0])
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}
