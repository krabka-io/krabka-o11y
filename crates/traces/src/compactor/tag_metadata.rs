use super::{BTreeMap, BTreeSet, RecordBatch, SCOL_INSTRUMENTATION_NAME, SCOL_INSTRUMENTATION_VERSION, TracesError, collect_attr_metadata, collect_event_metadata, collect_link_metadata, collect_string_column_metadata};

pub(crate) type TagMetadata = (BTreeSet<String>, BTreeMap<String, BTreeSet<String>>);

pub(crate) fn tag_metadata(batches: &[RecordBatch]) -> Result<TagMetadata, TracesError> {
    let mut tag_names = BTreeSet::new();
    let mut tag_values = BTreeMap::new();
    for batch in batches {
        collect_attr_metadata(batch, &mut tag_names, &mut tag_values)?;
        collect_event_metadata(batch, &mut tag_names, &mut tag_values)?;
        collect_link_metadata(batch, &mut tag_names, &mut tag_values)?;
        collect_string_column_metadata(
            batch,
            SCOL_INSTRUMENTATION_NAME,
            "instrumentation:name",
            &mut tag_names,
            &mut tag_values,
        )?;
        collect_string_column_metadata(
            batch,
            SCOL_INSTRUMENTATION_VERSION,
            "instrumentation:version",
            &mut tag_names,
            &mut tag_values,
        )?;
    }
    Ok((tag_names, tag_values))
}
