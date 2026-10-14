use super::{HeaderMap, accepts_media_type};

pub(crate) fn wants_protobuf(headers: &HeaderMap) -> bool {
    accepts_media_type(headers, &["application/protobuf", "application/x-protobuf"])
}
