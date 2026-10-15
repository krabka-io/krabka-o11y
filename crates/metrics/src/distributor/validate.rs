use super::{DecodedExemplar, DecodedSeries, HeaderMap, Limits, WireError};

pub(super) const MAX_EXEMPLAR_LABEL_CODEPOINTS: usize = 128;

pub(crate) fn is_valid_label_name(name: &str) -> bool {
    !name.is_empty()
}

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
        for pair in &series.labels {
            let name = pair.name;
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


pub(crate) fn header_list_includes(value: &str, expected: &str) -> bool {
    value
        .split(',')
        .any(|item| item.trim().eq_ignore_ascii_case(expected))
}

pub(crate) fn require_snappy_encoding(headers: &HeaderMap) -> Result<(), WireError> {
    let encoding = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !header_list_includes(encoding, "snappy") {
        return Err(WireError::UnsupportedContentEncoding(encoding.to_string()));
    }
    Ok(())
}

pub(crate) fn require_otlp_protobuf_content_type(headers: &HeaderMap) -> Result<(), WireError> {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let base = content_type.split(';').next().unwrap_or_default().trim();
    if !base.eq_ignore_ascii_case("application/x-protobuf") {
        return Err(WireError::UnsupportedContentType(base.to_string()));
    }
    Ok(())
}
