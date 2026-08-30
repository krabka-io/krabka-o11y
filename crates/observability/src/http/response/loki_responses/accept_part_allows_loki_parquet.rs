use super::{LOKI_PARQUET_CONTENT_TYPE, accept_parameter_is_zero_quality};

pub(crate) fn accept_part_allows_loki_parquet(part: &str) -> bool {
    let mut pieces = part.trim().split(';');
    let Some(mime) = pieces.next() else {
        return false;
    };
    if !mime.trim().eq_ignore_ascii_case(LOKI_PARQUET_CONTENT_TYPE) {
        return false;
    }

    !pieces.any(accept_parameter_is_zero_quality)
}
