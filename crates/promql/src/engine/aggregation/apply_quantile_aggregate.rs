use super::{
    BTreeMap, InstantSample, LabelModifier, Labels, SampleValue, aggregate_labels, emit_info,
    emit_warning, histogram_ignored_in_aggregation_info, invalid_quantile_warning,
    is_valid_quantile, labels_key, quantile_value,
};

/// Shared `quantile(phi, v)` core over an already-evaluated instant vector.
///
/// This function backs both the interpreter
/// (`PromqlEngine::eval_quantile_aggregate`) and the operator path. It groups
/// the float samples by the `by`/`without` label set and returns the
/// phi-quantile of each group's values. [`quantile_value`] does the linear
/// interpolation in rank space. An empty group returns no row. This function
/// skips histogram-typed samples.
///
/// A `phi` outside `[0, 1]`, or a NaN `phi`, is NOT an error. Each group returns
/// the signed `+/-Inf` or `NaN` value that [`quantile_value`] returns, and the
/// function raises one `InvalidQuantileWarning`. This matches Prometheus and the
/// `histogram_quantile` family.
pub(crate) fn apply_quantile_aggregate(
    samples: Vec<InstantSample>,
    quantile: f64,
    modifier: Option<&LabelModifier>,
    time_ms: i64,
) -> Vec<InstantSample> {
    if !is_valid_quantile(quantile) {
        emit_warning(invalid_quantile_warning(quantile));
    }
    let mut groups: BTreeMap<String, (Labels, Vec<f64>, bool)> = BTreeMap::new();
    for sample in samples {
        let SampleValue::Float(value) = sample.value else {
            emit_info(histogram_ignored_in_aggregation_info("quantile"));
            continue;
        };
        let labels = aggregate_labels(&sample.labels, modifier);
        let group = groups
            .entry(labels_key(&labels))
            .or_insert_with(|| (labels, Vec::new(), false));
        group.1.push(value);
        group.2 |= sample.drop_name;
    }

    groups
        .into_values()
        .filter_map(|(labels, mut values, drop_name)| {
            quantile_value(quantile, &mut values).map(|value| InstantSample {
                labels,
                ts_ms: time_ms,
                value: SampleValue::Float(value),
                drop_name,
            })
        })
        .collect()
}
