
pub(crate) fn is_result_metadata_label(name: &str) -> bool {
    matches!(name, "__name__" | "__type__" | "__unit__")
}
