use super::*;

impl From<krabka_pprof::Heatmap> for pb::querier::v1::HeatmapSeries {
    fn from(value: krabka_pprof::Heatmap) -> Self {
        let step_ms = if value.time_buckets == 0 {
            0
        } else {
            (value.end_ms - value.start_ms)
                / i64::try_from(value.time_buckets).expect("bucket count fits i64")
        };
        let y_min = heatmap_y_mins(
            MinValue(value.min_value),
            MaxValue(value.max_value),
            value.value_buckets,
        );
        Self {
            labels: Vec::new(),
            slots: value
                .counts
                .into_iter()
                .enumerate()
                .map(|(bucket, counts)| pb::querier::v1::HeatmapSlot {
                    timestamp: value.start_ms
                        + (i64::try_from(bucket).expect("bucket index fits i64") + 1) * step_ms,
                    y_min: y_min.clone(),
                    counts: counts
                        .into_iter()
                        .map(|count| i32::try_from(count).unwrap_or(i32::MAX))
                        .collect(),
                    exemplars: Vec::new(),
                })
                .collect(),
        }
    }
}

impl From<krabka_pprof::LabeledHeatmap> for pb::querier::v1::HeatmapSeries {
    fn from(value: krabka_pprof::LabeledHeatmap) -> Self {
        let mut series = Self::from(value.heatmap);
        series.labels = label_pairs(value.labels);
        series
    }
}
