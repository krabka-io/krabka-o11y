use super::{InstantSample, PromqlError, Result, set_label_value};
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
    Ok(apply_join_samples(
        samples,
        dst,
        separator.as_bytes(),
        &source_names,
    ))
}

pub(crate) fn apply_join_samples(
    samples: Vec<InstantSample>,
    dst_label: &str,
    separator: &[u8],
    source_names: &[&str],
) -> Vec<InstantSample> {
    samples
        .into_iter()
        .map(|mut sample| {
            let mut value = Vec::new();
            for (index, name) in source_names.iter().enumerate() {
                if index > 0 {
                    value.extend_from_slice(separator);
                }
                if let Some(source) = sample.labels.get_value(name) {
                    value.extend_from_slice(source.as_bytes());
                }
            }
            sample.labels =
                set_label_value(&sample.labels, dst_label, crate::PromqlString::from(value));
            if dst_label == "__name__" {
                sample.drop_name = false;
            }
            sample
        })
        .collect()
}
