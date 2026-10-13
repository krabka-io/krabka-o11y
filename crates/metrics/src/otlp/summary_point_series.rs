use super::{
    DecodedSeries, ExtraLabel, PointFamily, ScalarPointSeries, ScalarSeries, SummaryDataPoint,
    ToPrimitive, append_count_and_sum,
};

pub(crate) fn summary_point_series(
    point: &SummaryDataPoint,
    family: &PointFamily<'_>,
) -> Vec<DecodedSeries> {
    let PointFamily {
        name,
        resource_attributes,
        metadata,
        strategy,
    } = *family;
    let scalar = ScalarPointSeries {
        resource_attributes,
        attributes: &point.attributes,
        strategy,
        time_unix_nano: point.time_unix_nano,
        start_time_unix_nano: point.start_time_unix_nano,
        metadata: metadata.cloned(),
    };
    let mut out = Vec::new();
    for quantile in &point.quantile_values {
        let quantile_value = quantile.quantile.to_string();
        out.push(scalar.series(ScalarSeries {
            name,
            extra_label: Some(ExtraLabel {
                label_name: "quantile",
                label_value: &quantile_value,
            }),
            sample_value: quantile.value,
            exemplars: Vec::new(),
        }));
    }
    append_count_and_sum(
        &mut out,
        &scalar,
        name,
        point.count.to_f64().unwrap_or(f64::MAX),
        Some(point.sum),
    );
    out
}
