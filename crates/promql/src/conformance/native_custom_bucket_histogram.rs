use super::{Line, NativeHistogram, ResetHint, Result, histogram_span};

pub(crate) fn native_custom_bucket_histogram(
    custom_values: &[f64],
    counts: &[f64],
    total: f64,
    sum: f64,
    line: Line<'_>,
) -> Result<NativeHistogram> {
    let positive_counts = if total == 0.0 {
        Vec::new()
    } else {
        counts.to_vec()
    };
    let positive_spans = histogram_span(0, positive_counts.len(), line)?;
    Ok(NativeHistogram {
        schema: -53,
        is_float: true,
        reset_hint: ResetHint::Unknown,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count: total,
        sum,
        positive_spans: positive_spans.into_iter().collect(),
        positive_counts,
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: Some(custom_values.to_vec()),
        start_timestamp_ms: None,
    })
}
