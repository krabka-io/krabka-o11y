use super::{Line, Result, NativeHistogram, parse_error, histogram_fields, parse_optional_histogram_i8, parse_optional_histogram_f64, parse_optional_histogram_buckets, parse_optional_histogram_i32, parse_optional_histogram_reset_hint, histogram_span};

pub(crate) fn parse_histogram_literal(src: &str, line: Line<'_>) -> Result<NativeHistogram> {
    let content = src
        .strip_prefix("{{")
        .and_then(|src| src.strip_suffix("}}"))
        .ok_or_else(|| parse_error(line, "invalid native histogram literal"))?;
    let fields = histogram_fields(content, line)?;
    let schema = parse_optional_histogram_i8(&fields, "schema", line)?.unwrap_or(0);
    let sum = parse_optional_histogram_f64(&fields, "sum", line)?.unwrap_or(0.0);
    let count = parse_optional_histogram_f64(&fields, "count", line)?.unwrap_or(0.0);
    let zero_count = parse_optional_histogram_f64(&fields, "z_bucket", line)?.unwrap_or(0.0);
    let zero_threshold = parse_optional_histogram_f64(&fields, "z_bucket_w", line)?.unwrap_or(0.0);
    let positive_counts =
        parse_optional_histogram_buckets(&fields, "buckets", line)?.unwrap_or_else(Vec::new);
    let positive_offset = parse_optional_histogram_i32(&fields, "offset", line)?.unwrap_or(0);
    let negative_counts =
        parse_optional_histogram_buckets(&fields, "n_buckets", line)?.unwrap_or_else(Vec::new);
    let negative_offset = parse_optional_histogram_i32(&fields, "n_offset", line)?.unwrap_or(0);
    let custom_values = parse_optional_histogram_buckets(&fields, "custom_values", line)?;
    let reset_hint = parse_optional_histogram_reset_hint(&fields, line)?;
    let positive_spans = histogram_span(positive_offset, positive_counts.len(), line)?;
    let negative_spans = histogram_span(negative_offset, negative_counts.len(), line)?;

    Ok(NativeHistogram {
        schema,
        is_float: true,
        reset_hint,
        zero_threshold,
        zero_count,
        count,
        sum,
        positive_spans: positive_spans.into_iter().collect(),
        positive_counts,
        negative_spans: negative_spans.into_iter().collect(),
        negative_counts,
        custom_values,
        start_timestamp_ms: None,
    })
}
