use super::{Array, Labels, MapArray, PromqlError, Result, StringArray};

pub(crate) fn append_exemplar_label_map(
    labels: &mut Labels,
    label_maps: &MapArray,
    row: usize,
) -> Result<()> {
    let entries = label_maps.value(row);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromqlError::Store("exemplar label map key column has wrong type".into()))?;
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            PromqlError::Store("exemplar label map value column has wrong type".into())
        })?;
    for index in 0..keys.len() {
        if !keys.is_null(index) && !values.is_null(index) {
            labels.insert(keys.value(index), values.value(index));
        }
    }
    Ok(())
}
