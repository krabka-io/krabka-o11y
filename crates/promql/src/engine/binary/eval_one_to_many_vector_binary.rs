use super::{
    BTreeMap, BTreeSet, BinModifier, BinaryOp, InstantSample, MissingSide, PromqlError, Result,
    apply_binary_fill_value, apply_binary_sample_value, binary_match_key, binary_returns_bool,
    copy_group_labels, labels_without_metric_name, one_to_one_binary_result_labels,
};

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
    let mut matched = BTreeSet::new();
    for right_sample in right {
        let key = binary_match_key(&right_sample.labels, modifier);
        let Some(left_sample) = left_by_key.get(&key) else {
            // Prometheus swaps vector sides for group_right, keeping the fill flags.
            let Some(lhs_fill) = modifier.and_then(|modifier| modifier.fill_values.rhs) else {
                continue;
            };
            let Some(value) =
                apply_binary_fill_value(&right_sample, lhs_fill, op, modifier, MissingSide::Left)?
            else {
                continue;
            };
            let preserves_name = op.is_comparison() && !binary_returns_bool(modifier);
            let drop_name = if preserves_name {
                right_sample.drop_name
            } else {
                true
            };
            let mut labels = if preserves_name {
                right_sample.labels
            } else {
                labels_without_metric_name(&right_sample.labels)
            };
            let filled_labels = one_to_one_binary_result_labels(&labels, modifier, false);
            copy_group_labels(&mut labels, &filled_labels, group_labels);
            out.push(InstantSample {
                labels,
                ts_ms: right_sample.ts_ms,
                value,
                drop_name,
            });
            continue;
        };
        matched.insert(key);
        let Some(value) = apply_binary_sample_value(left_sample, &right_sample, op, modifier)?
        else {
            continue;
        };
        let preserves_name = op.is_comparison() && !binary_returns_bool(modifier);
        let drop_name = if preserves_name {
            right_sample.drop_name
        } else {
            true
        };
        let mut labels = if preserves_name {
            right_sample.labels.clone()
        } else {
            labels_without_metric_name(&right_sample.labels)
        };
        copy_group_labels(&mut labels, &left_sample.labels, group_labels);
        out.push(InstantSample {
            labels,
            ts_ms: right_sample.ts_ms,
            value,
            drop_name,
        });
    }
    if let Some(rhs_fill) = modifier.and_then(|modifier| modifier.fill_values.lhs) {
        for (key, left_sample) in left_by_key {
            if matched.contains(&key) {
                continue;
            }
            let Some(value) =
                apply_binary_fill_value(&left_sample, rhs_fill, op, modifier, MissingSide::Right)?
            else {
                continue;
            };
            let mut labels = one_to_one_binary_result_labels(&left_sample.labels, modifier, false);
            copy_group_labels(&mut labels, &left_sample.labels, group_labels);
            out.push(InstantSample {
                labels,
                ts_ms: left_sample.ts_ms,
                value,
                drop_name: true,
            });
        }
    }
    Ok(out)
}
