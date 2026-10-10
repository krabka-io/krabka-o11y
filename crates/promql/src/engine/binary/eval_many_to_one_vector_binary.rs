use super::{
    BTreeSet, InstantSample, MissingSide, Result, VectorMatching, VectorOperands,
    apply_binary_fill_value, apply_binary_sample_value, binary_match_key, binary_returns_bool,
    copy_group_labels, fill_missing_right, index_by_match_key, labels_without_metric_name,
    one_to_one_binary_result_labels,
};

pub(crate) fn eval_many_to_one_vector_binary(
    operands: VectorOperands,
    matching: VectorMatching<'_>,
    group_labels: &[String],
) -> Result<Vec<InstantSample>> {
    let VectorOperands { left, right } = operands;
    let VectorMatching { op, modifier } = matching;
    let right_by_key = index_by_match_key(right, modifier, |key| {
        format!("many-to-one matching requires the right side to be unique for key `{key}`")
    })?;

    let mut out = Vec::new();
    let mut matched = BTreeSet::new();
    for left_sample in left {
        let key = binary_match_key(&left_sample.labels, modifier);
        let Some(right_sample) = right_by_key.get(&key) else {
            out.extend(fill_missing_right(&left_sample, matching, |fill| {
                let mut labels = if fill.preserves_name {
                    left_sample.labels.clone()
                } else {
                    labels_without_metric_name(&left_sample.labels)
                };
                let filled_labels = one_to_one_binary_result_labels(&labels, modifier, false);
                copy_group_labels(&mut labels, &filled_labels, group_labels);
                labels
            })?);
            continue;
        };
        matched.insert(key);
        let Some(value) = apply_binary_sample_value(&left_sample, right_sample, op, modifier)?
        else {
            continue;
        };
        let preserves_name = op.is_comparison() && !binary_returns_bool(modifier);
        let drop_name = if preserves_name {
            left_sample.drop_name
        } else {
            true
        };
        let mut labels = if preserves_name {
            left_sample.labels.clone()
        } else {
            labels_without_metric_name(&left_sample.labels)
        };
        copy_group_labels(&mut labels, &right_sample.labels, group_labels);
        out.push(InstantSample {
            labels,
            ts_ms: left_sample.ts_ms,
            value,
            drop_name,
        });
    }
    if let Some(lhs_fill) = modifier.and_then(|modifier| modifier.fill_values.lhs) {
        for (key, right_sample) in right_by_key {
            if matched.contains(&key) {
                continue;
            }
            let Some(value) =
                apply_binary_fill_value(&right_sample, lhs_fill, op, modifier, MissingSide::Left)?
            else {
                continue;
            };
            let mut labels = one_to_one_binary_result_labels(&right_sample.labels, modifier, false);
            copy_group_labels(&mut labels, &right_sample.labels, group_labels);
            out.push(InstantSample {
                labels,
                ts_ms: right_sample.ts_ms,
                value,
                drop_name: true,
            });
        }
    }
    Ok(out)
}
