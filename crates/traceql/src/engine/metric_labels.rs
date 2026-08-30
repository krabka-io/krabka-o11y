use super::*;

pub(crate) fn metric_labels(
    batch: &RecordBatch,
    row: usize,
    fields: &[Field],
) -> Result<Vec<(String, String)>> {
    fields
        .iter()
        .map(|field| {
            let column = metric_field_column(field)?;
            let value = metric_label_value(batch, &column, row)?;
            Ok((metric_label_key(field), value))
        })
        .collect()
}
