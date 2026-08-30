use super::{Uri, optional_usize_param};

pub(crate) fn q_filter_limit(
    uri: &Uri,
    max_traces: usize,
    autocomplete_limit: usize,
) -> Result<usize, String> {
    Ok(
        optional_usize_param(uri, "limit")?.map_or(usize::MAX, |limit| {
            if limit == 0 {
                max_traces.min(autocomplete_limit)
            } else {
                limit.min(max_traces).min(autocomplete_limit)
            }
        }),
    )
}
