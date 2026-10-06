use super::{InstantSample, PromqlError, Result};
use crate::PromqlString;

pub(crate) fn apply_byte_label_join(
    samples: Vec<InstantSample>,
    dst: &PromqlString,
    separator: &PromqlString,
    source_names: &[PromqlString],
) -> Result<Vec<InstantSample>> {
    let source_names = source_names
        .iter()
        .map(|name| {
            name.utf8().filter(|name| !name.is_empty()).ok_or_else(|| {
                PromqlError::Exec("invalid source label name in label_join()".to_owned())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let dst = dst.utf8().filter(|name| !name.is_empty()).ok_or_else(|| {
        PromqlError::Exec("invalid destination label name in label_join()".to_owned())
    })?;
    Ok(samples
        .into_iter()
        .map(|mut sample| {
            let mut value = Vec::new();
            for (index, name) in source_names.iter().enumerate() {
                if index > 0 {
                    value.extend_from_slice(separator.as_bytes());
                }
                if let Some(source) = sample.labels.get_value(name) {
                    value.extend_from_slice(source.as_bytes());
                }
            }
            sample.labels =
                super::set_label_value(&sample.labels, dst, crate::PromqlString::from(value));
            if dst == "__name__" {
                sample.drop_name = false;
            }
            sample
        })
        .collect())
}
