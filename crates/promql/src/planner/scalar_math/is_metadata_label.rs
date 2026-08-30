use super::*;

pub(crate) fn is_metadata_label(name: &str) -> bool {
    METADATA_LABELS.contains(&name)
}
