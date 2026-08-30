use super::{exponential_histogram_data_point, BucketSpan, OtlpError, BTreeMap, compact_spanned_histogram_counts, ToPrimitive};

pub(crate) fn downscaled_spans(
    buckets: Option<&exponential_histogram_data_point::Buckets>,
    source_schema: i32,
    target_schema: i32,
) -> Result<(Vec<BucketSpan>, Vec<f64>), OtlpError> {
    let Some(buckets) = buckets else {
        return Ok((Vec::new(), Vec::new()));
    };
    let shift = u32::try_from(source_schema - target_schema)
        .map_err(|_| OtlpError::Invalid("exponential histogram".into(), "invalid scale".into()))?;
    let divisor = 1_i32.checked_shl(shift).ok_or_else(|| {
        OtlpError::Invalid(
            "exponential histogram".into(),
            format!("scale {source_schema} cannot be downscaled to schema {target_schema}"),
        )
    })?;
    let mut merged = BTreeMap::<i32, u64>::new();
    for (idx, count) in buckets.bucket_counts.iter().enumerate() {
        let idx = i32::try_from(idx).map_err(|_| {
            OtlpError::Invalid("exponential histogram".into(), "too many buckets".into())
        })?;
        let original_offset = buckets.offset.checked_add(idx).ok_or_else(|| {
            OtlpError::Invalid(
                "exponential histogram".into(),
                "bucket offset overflow".into(),
            )
        })?;
        let offset = original_offset
            .div_euclid(divisor)
            .checked_add(1)
            .ok_or_else(|| {
                OtlpError::Invalid(
                    "exponential histogram".into(),
                    "bucket offset overflow".into(),
                )
            })?;
        let merged_count = merged.entry(offset).or_default();
        if target_schema < source_schema && *merged_count != 0 && *count != 0 {
            return Err(OtlpError::Invalid(
                "exponential histogram".into(),
                format!(
                    "scale {source_schema} cannot be downscaled to schema {target_schema} without lossy downscale"
                ),
            ));
        }
        *merged_count += count;
    }

    // Counts are carried as f64 downstream, and the span encoder is shared
    // with the merge path so both produce the same delta-offset form.
    Ok(compact_spanned_histogram_counts(
        merged
            .into_iter()
            .map(|(index, count)| (index, count.to_f64().unwrap_or(f64::MAX)))
            .collect(),
    ))
}
