use super::{
    Array, BTreeMap, BTreeSet, BooleanArray, Float64Array, Int64Array, ListArray,
    RESOURCE_ATTR_PREFIX, RecordBatch, SCOL_ATTR_KEYS, SCOL_EVENTS, SCOL_INSTRUMENTATION_NAME,
    SCOL_INSTRUMENTATION_VERSION, StringArray, TagCatalog, TracesError, attr_value,
    collect_link_metadata, collect_nested_column_metadata, collect_string_column_metadata,
    list_column, string_list_value, struct_i64_field, struct_list_field, struct_string_field,
};

pub(crate) trait MetadataValueArray: Array {
    fn string_value(&self, idx: usize) -> String;
}

impl MetadataValueArray for BooleanArray {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}

impl MetadataValueArray for Float64Array {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}

impl MetadataValueArray for Int64Array {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}

impl MetadataValueArray for StringArray {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}

pub(crate) fn collect_nested_attrs(
    keys: &ListArray,
    values: &ListArray,
    idx: usize,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    let mut catalog = TagCatalog {
        names: tag_names,
        values: tag_values,
    };
    if keys.is_null(idx) {
        return Ok(());
    }
    let attr_keys = keys.value(idx);
    let attr_keys = attr_keys
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TracesError::Block("nested attr keys are not Utf8".into()))?;
    let attr_values = if values.is_null(idx) {
        None
    } else {
        Some(values.value(idx))
    };
    let attr_values = attr_values
        .as_ref()
        .and_then(|array| array.as_any().downcast_ref::<ListArray>());

    for attr_idx in 0..attr_keys.len() {
        if attr_keys.is_null(attr_idx) {
            continue;
        }
        let key = attr_keys.value(attr_idx);
        if let Some(value) = attr_values.and_then(|values| string_list_value(values, attr_idx)) {
            catalog.insert(key, value);
        } else {
            catalog.names.insert(key.to_string());
        }
    }
    Ok(())
}

pub(crate) fn collect_attr_metadata(
    batch: &RecordBatch,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    let mut catalog = TagCatalog {
        names: tag_names,
        values: tag_values,
    };
    let keys = list_column(batch, SCOL_ATTR_KEYS)?;
    for row in 0..batch.num_rows() {
        if keys.is_null(row) {
            continue;
        }
        let row_keys = keys.value(row);
        let row_keys = row_keys
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| TracesError::Block("attr_keys row is not Utf8".into()))?;
        for idx in 0..row_keys.len() {
            if row_keys.is_null(idx) {
                continue;
            }
            let key = row_keys
                .value(idx)
                .strip_prefix(RESOURCE_ATTR_PREFIX)
                .unwrap_or_else(|| row_keys.value(idx));
            if let Some(value) = attr_value(batch, row, idx)? {
                catalog.insert(key, value);
            } else {
                catalog.names.insert(key.to_string());
            }
        }
    }
    Ok(())
}

pub(crate) fn collect_event_metadata(
    batch: &RecordBatch,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    collect_nested_column_metadata(
        batch,
        SCOL_EVENTS,
        tag_names,
        tag_values,
        |event, catalog| {
            let names = struct_string_field(event, 0)?;
            let times = struct_i64_field(event, 1)?;
            let keys = struct_list_field(event, 2)?;
            let values = struct_list_field(event, 3)?;
            for idx in 0..event.len() {
                if event.is_null(idx) {
                    continue;
                }
                if !names.is_null(idx) {
                    catalog.insert("event:name", names.value(idx).to_string());
                }
                if !times.is_null(idx) {
                    catalog.insert("event:timeSinceStart", times.value(idx).to_string());
                }
                collect_nested_attrs(keys, values, idx, catalog.names, catalog.values)?;
            }
            Ok(())
        },
    )
}

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
