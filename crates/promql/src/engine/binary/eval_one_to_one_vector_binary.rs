use super::{
    BTreeSet, InstantSample, MissingSide, PromqlError, Result, VectorMatching, VectorOperands,
    apply_binary_fill_value, apply_binary_sample_value, binary_match_key, binary_returns_bool,
    fill_missing_right, index_by_match_key, one_to_one_binary_result_labels,
};

pub(crate) fn eval_one_to_one_vector_binary(
    operands: VectorOperands,
    matching: VectorMatching<'_>,
) -> Result<Vec<InstantSample>> {
    let VectorOperands { left, right } = operands;
    let VectorMatching { op, modifier } = matching;
    let right_by_key = index_by_match_key(right, modifier, |key| {
        format!("many-to-one matching for key `{key}` is not supported")
    })?;

    let mut out = Vec::new();
    let mut matched_keys = BTreeSet::new();
    for left_sample in left {
        let key = binary_match_key(&left_sample.labels, modifier);
        let Some(right_sample) = right_by_key.get(&key) else {
            out.extend(fill_missing_right(&left_sample, matching, |fill| {
                one_to_one_binary_result_labels(&left_sample.labels, modifier, fill.preserves_name)
            })?);
            continue;
        };
        if !matched_keys.insert(key.clone()) {
            return Err(PromqlError::Exec(format!(
                "many-to-one matching must be explicit for key `{key}`"
            )));
        }
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
        let labels = one_to_one_binary_result_labels(&left_sample.labels, modifier, preserves_name);
        out.push(InstantSample {
            labels,
            ts_ms: left_sample.ts_ms,
            value,
            drop_name,
        });
    }
    if let Some(lhs_fill) = modifier.and_then(|modifier| modifier.fill_values.lhs) {
        for (key, right_sample) in right_by_key {
            if matched_keys.contains(&key) {
                continue;
            }
            let Some(value) =
                apply_binary_fill_value(&right_sample, lhs_fill, op, modifier, MissingSide::Left)?
            else {
                continue;
            };
            // The synthetic left side has no metric name, including filtered comparisons.
            let drop_name = true;
            let labels = one_to_one_binary_result_labels(&right_sample.labels, modifier, false);
            out.push(InstantSample {
                labels,
                ts_ms: right_sample.ts_ms,
                value,
                drop_name,
            });
        }
    }
    Ok(out)
}
