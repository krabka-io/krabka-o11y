/// Result-metadata labels that every scalar-math function drops immediately.
///
/// This list mirrors the interpreter function `is_result_metadata_label`. These
/// labels never reach the leaf, so the projection drops them implicitly. The
/// metric name remains until the outer query boundary.
pub(crate) const METADATA_LABELS: [&str; 2] = ["__type__", "__unit__"];
