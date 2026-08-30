use super::*;

pub(crate) fn append_exemplar_label_map(labels: &mut Labels, label_maps: &MapArray, row: usize) -> Result<()> {
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
    for (key, value) in keys.iter().zip(values.iter()) {
        if let (Some(key), Some(value)) = (key, value) {
            labels.insert(key, value);
        }
    }
    Ok(())
}
