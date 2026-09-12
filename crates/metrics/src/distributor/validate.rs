use super::{DecodedSeries, Limits, WireError, is_valid_label_name, validate_exemplar_labels};

/// Validates the shape of the decoded series against the tenant's limits.
///
/// Label lengths are not checked here. `enforce_label_limits` applies them,
/// from the same resolved [`Limits`], so one request gets one verdict on a
/// label and gets it in Mimir's error shape.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn validate(series: &[DecodedSeries], limits: &Limits) -> Result<(), WireError> {
    let series_count = u64::try_from(series.len()).unwrap_or(u64::MAX);
    if series_count > limits.max_series_per_request {
        return Err(WireError::Invalid(format!(
            "series per request {series_count} exceeds limit {}",
            limits.max_series_per_request
        )));
    }

    for series in series {
        if series.labels.get("__name__").is_none_or(str::is_empty) {
            return Err(WireError::Invalid("missing metric name".into()));
        }
        let sample_count = series.samples.len() + series.histograms.len() + series.exemplars.len();
        let sample_count = u64::try_from(sample_count).unwrap_or(u64::MAX);
        if sample_count > limits.max_samples_per_series {
            return Err(WireError::Invalid(format!(
                "samples per series {sample_count} exceeds limit {}",
                limits.max_samples_per_series
            )));
        }
        for (name, _) in series.labels.iter() {
            if !is_valid_label_name(name) {
                return Err(WireError::Invalid(format!("invalid label name `{name}`")));
            }
        }
        for exemplar in &series.exemplars {
            validate_exemplar_labels(exemplar)?;
        }
    }

    Ok(())
}
