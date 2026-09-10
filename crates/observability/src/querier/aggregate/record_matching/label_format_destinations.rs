use super::{BTreeSet, LabelFormatAssignment, PipelineStage, StreamQuery};

/// The label names a `label_format` stage of `query` writes.
///
/// Loki categorises such a label as parsed whatever it held before -- a series
/// label, a piece of structured metadata, or nothing at all -- and it does so
/// even when the value written is the one the label already carried. Krabka's
/// pipeline hands back one flat field map, in which a rewrite to the same
/// string is indistinguishable from no rewrite, so the query is asked instead
/// of the value.
pub(crate) fn label_format_destinations(query: &StreamQuery) -> BTreeSet<&str> {
    query
        .pipeline
        .iter()
        .filter_map(|stage| match stage {
            PipelineStage::LabelFormat(format) => Some(format.assignments()),
            _ => None,
        })
        .flatten()
        .map(LabelFormatAssignment::destination)
        .collect()
}
