use super::{InstantSample, LabelModifier, Result, BTreeMap, AggregateState, aggregate_labels, count_values_label_value, labels_key, SampleValue};

/// Shared `count_values("label", v)` core over an already-evaluated instant
/// vector.
///
/// This function backs both the interpreter
/// (`PromqlEngine::eval_count_values_aggregate`) and the operator path. It
/// groups by the `by`/`without` label set, extended with the named label, which
/// it sets to each sample's formatted value. Floats use `Display` and histograms
/// use JSON. The function returns one series per distinct value, and each series
/// carries the group's count. It returns `Err` only when it cannot encode a
/// histogram value.
pub(crate) fn apply_count_values_aggregate(
    samples: Vec<InstantSample>,
    label_name: &str,
    modifier: Option<&LabelModifier>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let mut groups = BTreeMap::<String, AggregateState>::new();
    for sample in samples {
        let mut labels = aggregate_labels(&sample.labels, modifier);
        labels.insert(label_name, count_values_label_value(&sample.value)?);
        groups
            .entry(labels_key(&labels))
            .or_insert_with(|| AggregateState::new(labels))
            .push_float(1.0);
    }

    Ok(groups
        .into_values()
        .map(|state| InstantSample {
            labels: state.labels,
            ts_ms: time_ms,
            value: SampleValue::Float(state.count_f64),
        })
        .collect())
}
