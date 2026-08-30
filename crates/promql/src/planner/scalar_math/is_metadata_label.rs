use super::METADATA_LABELS;

pub(crate) fn is_metadata_label(name: &str) -> bool {
    METADATA_LABELS.contains(&name)
}
