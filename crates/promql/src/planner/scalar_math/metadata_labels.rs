
/// Result-metadata labels that every scalar-math function drops.
///
/// This list mirrors the interpreter function `is_result_metadata_label`. These
/// labels never reach the leaf, so the projection drops them implicitly.
pub(crate) const METADATA_LABELS: [&str; 3] = ["__name__", "__type__", "__unit__"];
