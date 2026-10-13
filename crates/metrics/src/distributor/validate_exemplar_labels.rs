use super::{DecodedExemplar, MAX_EXEMPLAR_LABEL_CODEPOINTS, WireError, is_valid_label_name};

pub(crate) fn validate_exemplar_labels(exemplar: &DecodedExemplar) -> Result<(), WireError> {
    let codepoints = exemplar
        .labels
        .iter()
        .try_fold(0usize, |codepoints, label| {
            if !is_valid_label_name(label.name) {
                return Err(WireError::Invalid(format!(
                    "invalid exemplar label name `{}`",
                    label.name
                )));
            }
            Ok(codepoints + label.name.chars().count() + label.value.chars().count())
        })?;
    if codepoints > MAX_EXEMPLAR_LABEL_CODEPOINTS {
        return Err(WireError::Invalid(format!(
            "exemplar label set has {codepoints} codepoints, exceeding limit {MAX_EXEMPLAR_LABEL_CODEPOINTS}"
        )));
    }
    Ok(())
}
