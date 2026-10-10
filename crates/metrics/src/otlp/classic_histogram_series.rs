use super::{
    DecodedSeries, ExtraLabel, HistogramDataPoint, OtlpError, PointFamily, ScalarPointSeries,
    ScalarSeries, ToPrimitive, exemplars_for_bucket, exemplars_from_histogram_point,
};

pub(crate) fn classic_histogram_series(
    point: &HistogramDataPoint,
    family: &PointFamily<'_>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let PointFamily {
        name,
        resource_attributes,
        metadata,
        strategy,
    } = *family;
    if !point.bucket_counts.is_empty()
        && point.bucket_counts.len() != point.explicit_bounds.len() + 1
    {
        return Err(OtlpError::Invalid(
            name.into(),
            "bucket_counts length must be explicit_bounds length plus one".into(),
        ));
    }

    let scalar = ScalarPointSeries {
        resource_attributes,
        attributes: &point.attributes,
        strategy,
        time_unix_nano: point.time_unix_nano,
        start_time_unix_nano: point.start_time_unix_nano,
        metadata: metadata.cloned(),
    };
    let point_exemplars = exemplars_from_histogram_point(point, strategy);
    let mut out = Vec::new();
    let base_name = format!("{name}_bucket");
    let mut cumulative = 0_u64;
    for (idx, count) in point.bucket_counts.iter().enumerate() {
        cumulative = cumulative.saturating_add(*count);
        let le = point
            .explicit_bounds
            .get(idx)
            .map_or_else(|| "+Inf".to_string(), ToString::to_string);
        out.push(scalar.series(ScalarSeries {
            name: &base_name,
            extra_label: Some(ExtraLabel {
                label_name: "le",
                label_value: &le,
            }),
            sample_value: cumulative.to_f64().unwrap_or(f64::MAX),
            exemplars: exemplars_for_bucket(&point_exemplars, point, idx),
        }));
    }

    out.push(scalar.series(ScalarSeries {
        name: &format!("{name}_count"),
        extra_label: None,
        sample_value: point.count.to_f64().unwrap_or(f64::MAX),
        exemplars: Vec::new(),
    }));
    if let Some(sum) = point.sum {
        out.push(scalar.series(ScalarSeries {
            name: &format!("{name}_sum"),
            extra_label: None,
            sample_value: sum,
            exemplars: Vec::new(),
        }));
    }
    Ok(out)
}
