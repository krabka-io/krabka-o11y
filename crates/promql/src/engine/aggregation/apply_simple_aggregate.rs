use super::{InstantSample, AggregateOp, LabelModifier, Result, BTreeMap, AggregateState, aggregate_labels, labels_key, SampleValue, PromqlError};

/// Shared simple-aggregation core over an already-evaluated instant vector.
///
/// The simple ops are `sum`, `avg`, `count`, `group`, `min`, `max`, `stddev`,
/// and `stdvar`.
///
/// This function backs both the interpreter
/// (`PromqlEngine::eval_instant_aggregate`) and the operator path
/// (`PromqlEngine::plan_aggregate_with_grouping`), so the two are identical by
/// construction once their inputs match. It groups the samples by the
/// `by`/`without` label set, accumulates each group's [`AggregateState`], and
/// returns one reduced sample per surviving group.
///
/// [`AggregateState`] and [`AggregateOp`] hold all the native-histogram rules,
/// which match Prometheus exactly:
/// - `sum`/`avg` (`aggregates_histograms`): histogram samples are MERGED. `sum`
///   adds them, and `avg` scales the merged histogram by `1/count`. A group that
///   mixes a float and a histogram is marked invalid and DROPPED from the
///   output through the `invalid_mixed_sample_type` flag. This happens when a
///   float arrives after a histogram
///   ([`AggregateState::mark_invalid_mixed_sample_type`]) or when a histogram
///   arrives after a float ([`AggregateState::push_histogram`]).
/// - `count`/`group` (`counts_histograms`): every sample is counted, whatever
///   its type. Histograms go through [`AggregateState::push_observation`].
/// - `min`/`max`/`stddev`/`stdvar` (`ignores_histograms`): histogram samples are
///   dropped with no annotation, exactly as the interpreter ignores them. This
///   matches Prometheus.
///
/// This function returns `Err` only for the unreachable case of a histogram
/// sample under an op that does not aggregate, count, or ignore histograms.
/// Every [`AggregateOp`] is in one of those three groups, so this branch mirrors
/// the interpreter's identical defensive branch.
pub(crate) fn apply_simple_aggregate(
    samples: Vec<InstantSample>,
    op: AggregateOp,
    modifier: Option<&LabelModifier>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let mut groups: BTreeMap<String, AggregateState> = BTreeMap::new();
    for sample in samples {
        let labels = aggregate_labels(&sample.labels, modifier);
        let state = groups
            .entry(labels_key(&labels))
            .or_insert_with(|| AggregateState::new(labels));
        match sample.value {
            SampleValue::Float(value) => {
                if op.aggregates_histograms() && state.has_histogram() {
                    state.mark_invalid_mixed_sample_type();
                    continue;
                }
                state.push_float(value);
            }
            SampleValue::Histogram(histogram) if op.aggregates_histograms() => {
                state.push_histogram(histogram)?;
            }
            SampleValue::Histogram(_) if op.counts_histograms() => state.push_observation(),
            SampleValue::Histogram(_) if op.ignores_histograms() => {}
            SampleValue::Histogram(_) => {
                return Err(PromqlError::Plan(
                    "native histogram reached an invalid aggregate classification".to_string(),
                ));
            }
        }
    }

    Ok(groups
        .into_values()
        .filter_map(|state| {
            op.finish(&state).map(|value| InstantSample {
                labels: state.labels,
                ts_ms: time_ms,
                value,
            })
        })
        .collect())
}
