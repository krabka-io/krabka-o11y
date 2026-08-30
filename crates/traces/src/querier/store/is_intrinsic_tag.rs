use super::*;

pub(crate) fn is_intrinsic_tag(tag: &str) -> bool {
    tag.contains(':')
}
