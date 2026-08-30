use super::*;

pub(crate) fn trace_ids(resp: &SearchResponse) -> Vec<u8> {
    resp.traces.iter().map(|trace| trace.trace_id[0]).collect()
}
