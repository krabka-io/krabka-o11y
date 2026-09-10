use super::{
    BTreeMap, BTreeSet, RecordBatch, SCOL_INSTRUMENTATION_NAME, SCOL_INSTRUMENTATION_VERSION,
    TracesError, collect_attr_metadata, collect_event_metadata, collect_link_metadata,
    collect_string_column_metadata,
};

/// Folds `batch`'s searchable tags into the sets a compacted block's index
/// entry carries.
///
/// The tag names and values of a block are unions over its rows, so they
/// accumulate as the merged batches go past and never need the block resident.
///
/// # Errors
/// Returns [`TracesError::Block`] when a batch does not carry the span block's
/// attribute columns, or carries them at the wrong type.
pub(crate) fn push_tag_metadata(
    batch: &RecordBatch,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    collect_attr_metadata(batch, tag_names, tag_values)?;
    collect_event_metadata(batch, tag_names, tag_values)?;
    collect_link_metadata(batch, tag_names, tag_values)?;
    collect_string_column_metadata(
        batch,
        SCOL_INSTRUMENTATION_NAME,
        "instrumentation:name",
        tag_names,
        tag_values,
    )?;
    collect_string_column_metadata(
        batch,
        SCOL_INSTRUMENTATION_VERSION,
        "instrumentation:version",
        tag_names,
        tag_values,
    )
}
