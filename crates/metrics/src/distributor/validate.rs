use super::*;

/// Validates the decoded series against the structural limits.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn validate(series: &[DecodedSeries], limits: &TenantLimits) -> Result<(), WireError> {
    if series.len() > limits.max_series_per_request {
        return Err(WireError::Invalid(format!(
            "series per request {} exceeds limit {}",
            series.len(),
            limits.max_series_per_request
        )));
    }

    for series in series {
        let sample_count = series.samples.len() + series.histograms.len() + series.exemplars.len();
        if sample_count > limits.max_samples_per_series {
            return Err(WireError::Invalid(format!(
                "samples per series {sample_count} exceeds limit {}",
                limits.max_samples_per_series
            )));
        }
        for (name, value) in series.labels.iter() {
            if !is_valid_label_name(name) {
                return Err(WireError::Invalid(format!("invalid label name `{name}`")));
            }
            let name_limit = limits.max_label_name_len.bytes_usize();
            if name.len() > name_limit {
                return Err(WireError::Invalid(format!(
                    "label name length {} exceeds limit {name_limit}",
                    name.len(),
                )));
            }
            let value_limit = limits.max_label_value_len.bytes_usize();
            if value.len() > value_limit {
                return Err(WireError::Invalid(format!(
                    "label value length {} exceeds limit {value_limit}",
                    value.len(),
                )));
            }
        }
        for exemplar in &series.exemplars {
            validate_exemplar_labels(exemplar)?;
        }
    }

    Ok(())
}
