use super::*;

pub(crate) fn is_nested_intrinsic_tag(tag: &str) -> bool {
    matches!(
        tag,
        "event:name" | "event:timeSinceStart" | "link:traceID" | "link:spanID"
    )
}
