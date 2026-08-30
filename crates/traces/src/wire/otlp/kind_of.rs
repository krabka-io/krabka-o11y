use super::{OtlpKind, SpanKind};

pub(crate) fn kind_of(kind: i32) -> SpanKind {
    if kind == OtlpKind::Unspecified as i32 {
        SpanKind::Unspecified
    } else {
        SpanKind::from_i32(kind)
    }
}
