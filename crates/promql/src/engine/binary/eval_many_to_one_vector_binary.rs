use super::*;

pub(crate) fn eval_many_to_one_vector_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
    group_labels: &[String],
) -> Result<Vec<InstantSample>> {
    let mut right_by_key: BTreeMap<String, InstantSample> = BTreeMap::new();
    for sample in right {
        let key = binary_match_key(&sample.labels, modifier);
        if right_by_key.insert(key.clone(), sample).is_some() {
            return Err(PromqlError::Exec(format!(
                "many-to-one matching requires the right side to be unique for key `{key}`"
            )));
        }
    }

    let mut out = Vec::new();
    for left_sample in left {
        let key = binary_match_key(&left_sample.labels, modifier);
        let Some(right_sample) = right_by_key.get(&key) else {
            let Some(rhs_fill) = modifier.and_then(|modifier| modifier.fill_values.rhs) else {
                continue;
            };
            let Some(value) =
                apply_binary_fill_value(&left_sample, rhs_fill, op, modifier, MissingSide::Right)?
            else {
                continue;
            };
            let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
                left_sample.labels
            } else {
                labels_without_metric_name(&left_sample.labels)
            };
            out.push(InstantSample {
                labels,
                ts_ms: left_sample.ts_ms,
                value,
            });
            continue;
        };
        let Some(value) = apply_binary_sample_value(&left_sample, right_sample, op, modifier)?
        else {
            continue;
        };
        let mut labels = if op.is_comparison() && !binary_returns_bool(modifier) {
            left_sample.labels.clone()
        } else {
            labels_without_metric_name(&left_sample.labels)
        };
        copy_group_labels(&mut labels, &right_sample.labels, group_labels);
        out.push(InstantSample {
            labels,
            ts_ms: left_sample.ts_ms,
            value,
        });
    }
    Ok(out)
}
