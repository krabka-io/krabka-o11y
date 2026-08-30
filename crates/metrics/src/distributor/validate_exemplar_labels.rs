use super::*;

pub(crate) fn validate_exemplar_labels(exemplar: &DecodedExemplar) -> Result<(), WireError> {
    let codepoints = exemplar
        .labels
        .iter()
        .try_fold(0usize, |codepoints, (name, value)| {
            if !is_valid_label_name(name) {
                return Err(WireError::Invalid(format!(
                    "invalid exemplar label name `{name}`"
                )));
            }
            Ok(codepoints + name.chars().count() + value.chars().count())
        })?;
    if codepoints > MAX_EXEMPLAR_LABEL_CODEPOINTS {
        return Err(WireError::Invalid(format!(
            "exemplar label set has {codepoints} codepoints, exceeding limit {MAX_EXEMPLAR_LABEL_CODEPOINTS}"
        )));
    }
    Ok(())
}
