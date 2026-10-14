use super::{
    InstantSample, Result, VectorMatching, VectorOperands, append_left_fill_results,
    append_left_match_results, copy_group_labels, index_by_match_key, labels_without_metric_name,
    one_to_one_binary_result_labels,
};

pub(crate) fn eval_many_to_one_vector_binary(
    operands: VectorOperands,
    matching: VectorMatching<'_>,
    group_labels: &[String],
) -> Result<Vec<InstantSample>> {
    let VectorOperands { left, right } = operands;
    let modifier = matching.modifier;
    let right_by_key = index_by_match_key(right, modifier, |key| {
        format!("many-to-one matching requires the right side to be unique for key `{key}`")
    })?;

    let mut left_matches = append_left_match_results(
        left,
        &right_by_key,
        matching,
        false,
        |left, preserves_name| {
            let mut labels = if preserves_name {
                left.labels.clone()
            } else {
                labels_without_metric_name(&left.labels)
            };
            let filled_labels = one_to_one_binary_result_labels(&labels, modifier, false);
            copy_group_labels(&mut labels, &filled_labels, group_labels);
            labels
        },
        |left, right, preserves_name| {
            let mut labels = if preserves_name {
                left.labels.clone()
            } else {
                labels_without_metric_name(&left.labels)
            };
            copy_group_labels(&mut labels, &right.labels, group_labels);
            labels
        },
    )?;
    append_left_fill_results(
        &mut left_matches.samples,
        right_by_key,
        &left_matches.matched_keys,
        modifier.and_then(|modifier| modifier.fill_values.lhs),
        matching.op,
        modifier,
        |sample| {
            let mut labels = one_to_one_binary_result_labels(&sample.labels, modifier, false);
            copy_group_labels(&mut labels, &sample.labels, group_labels);
            labels
        },
    )?;
    Ok(left_matches.samples)
}
