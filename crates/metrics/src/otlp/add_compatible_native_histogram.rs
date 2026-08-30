use super::*;

pub(crate) fn add_compatible_native_histogram(
    metric_name: &str,
    cumulative: &mut NativeHistogram,
    delta: &NativeHistogram,
) -> Result<(), OtlpError> {
    if cumulative.schema != delta.schema
        || cumulative.is_float != delta.is_float
        || cumulative.reset_hint != delta.reset_hint
        || cumulative.zero_threshold.to_bits() != delta.zero_threshold.to_bits()
        || cumulative.custom_values != delta.custom_values
    {
        return Err(OtlpError::Invalid(
            metric_name.into(),
            "incompatible delta exponential histogram layout".into(),
        ));
    }

    cumulative.zero_count += delta.zero_count;
    cumulative.count += delta.count;
    cumulative.sum += delta.sum;
    (cumulative.positive_spans, cumulative.positive_counts) = add_spanned_histogram_counts(
        &cumulative.positive_spans,
        &cumulative.positive_counts,
        &delta.positive_spans,
        &delta.positive_counts,
    );
    (cumulative.negative_spans, cumulative.negative_counts) = add_spanned_histogram_counts(
        &cumulative.negative_spans,
        &cumulative.negative_counts,
        &delta.negative_spans,
        &delta.negative_counts,
    );
    Ok(())
}
