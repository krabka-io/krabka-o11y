use super::{HeaderMap, accepts_media_type};

pub(crate) fn wants_json(headers: &HeaderMap) -> bool {
    accepts_media_type(headers, &["application/json"])
}
