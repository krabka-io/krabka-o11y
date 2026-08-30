use super::{Array, ListArray, StructArray, TracesError};

pub(crate) fn collect_nested_metadata(
    values: &ListArray,
    mut collect: impl FnMut(&StructArray) -> Result<(), TracesError>,
) -> Result<(), TracesError> {
    for row in 0..values.len() {
        if values.is_null(row) {
            continue;
        }
        let nested = values.value(row);
        let nested = nested
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| TracesError::Block("nested metadata row is not a struct".into()))?;
        collect(nested)?;
    }
    Ok(())
}
