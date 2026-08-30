use super::{ACCEPT, HeaderMap, accept_part_allows_loki_parquet};

pub(crate) fn wants_loki_parquet(headers: &HeaderMap) -> bool {
    headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.split(',').any(accept_part_allows_loki_parquet))
}
