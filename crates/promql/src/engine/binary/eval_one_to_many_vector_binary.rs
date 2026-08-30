use super::{InstantSample, BinaryOp, BinModifier, Result, BTreeMap, binary_match_key, PromqlError, apply_binary_fill_value, MissingSide, binary_returns_bool, labels_without_metric_name, apply_binary_sample_value, copy_group_labels};

pub(crate) fn eval_one_to_many_vector_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
    group_labels: &[String],
) -> Result<Vec<InstantSample>> {
    let mut left_by_key: BTreeMap<String, InstantSample> = BTreeMap::new();
    for sample in left {
        let key = binary_match_key(&sample.labels, modifier);
        if left_by_key.insert(key.clone(), sample).is_some() {
            return Err(PromqlError::Exec(format!(
                "one-to-many matching requires the left side to be unique for key `{key}`"
            )));
        }
    }

    let mut out = Vec::new();
    for right_sample in right {
        let key = binary_match_key(&right_sample.labels, modifier);
        let Some(left_sample) = left_by_key.get(&key) else {
            let Some(lhs_fill) = modifier.and_then(|modifier| modifier.fill_values.lhs) else {
                continue;
            };
            let Some(value) =
                apply_binary_fill_value(&right_sample, lhs_fill, op, modifier, MissingSide::Left)?
            else {
                continue;
            };
            let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
                right_sample.labels
            } else {
                labels_without_metric_name(&right_sample.labels)
            };
            out.push(InstantSample {
                labels,
                ts_ms: right_sample.ts_ms,
                value,
            });
            continue;
        };
        let Some(value) = apply_binary_sample_value(left_sample, &right_sample, op, modifier)?
        else {
            continue;
        };
        let mut labels = if op.is_comparison() && !binary_returns_bool(modifier) {
            right_sample.labels.clone()
        } else {
            labels_without_metric_name(&right_sample.labels)
        };
        copy_group_labels(&mut labels, &left_sample.labels, group_labels);
        out.push(InstantSample {
            labels,
            ts_ms: right_sample.ts_ms,
            value,
        });
    }
    Ok(out)
}
