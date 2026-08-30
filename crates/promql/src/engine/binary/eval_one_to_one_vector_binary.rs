use super::{
    BTreeMap, BinModifier, BinaryOp, InstantSample, MissingSide, PromqlError, Result,
    apply_binary_fill_value, apply_binary_sample_value, binary_match_key, binary_returns_bool,
    one_to_one_binary_result_labels,
};

pub(crate) fn eval_one_to_one_vector_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Vec<InstantSample>> {
    let mut right_by_key: BTreeMap<String, InstantSample> = BTreeMap::new();
    for sample in right {
        let key = binary_match_key(&sample.labels, modifier);
        if right_by_key.insert(key.clone(), sample).is_some() {
            return Err(PromqlError::Exec(format!(
                "many-to-one matching for key `{key}` is not supported"
            )));
        }
    }

    let mut out = Vec::new();
    for left_sample in left {
        let key = binary_match_key(&left_sample.labels, modifier);
        let Some(right_sample) = right_by_key.remove(&key) else {
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
                one_to_one_binary_result_labels(&left_sample.labels, modifier)
            };
            out.push(InstantSample {
                labels,
                ts_ms: left_sample.ts_ms,
                value,
            });
            continue;
        };
        let Some(value) = apply_binary_sample_value(&left_sample, &right_sample, op, modifier)?
        else {
            continue;
        };
        let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
            left_sample.labels
        } else {
            one_to_one_binary_result_labels(&left_sample.labels, modifier)
        };
        out.push(InstantSample {
            labels,
            ts_ms: left_sample.ts_ms,
            value,
        });
    }
    if let Some(lhs_fill) = modifier.and_then(|modifier| modifier.fill_values.lhs) {
        for right_sample in right_by_key.into_values() {
            let Some(value) =
                apply_binary_fill_value(&right_sample, lhs_fill, op, modifier, MissingSide::Left)?
            else {
                continue;
            };
            let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
                right_sample.labels
            } else {
                one_to_one_binary_result_labels(&right_sample.labels, modifier)
            };
            out.push(InstantSample {
                labels,
                ts_ms: right_sample.ts_ms,
                value,
            });
        }
    }
    Ok(out)
}
