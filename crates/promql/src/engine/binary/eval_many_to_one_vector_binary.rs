use super::{
    BTreeSet, InstantSample, LeftSampleMatch, Result, VectorMatching, VectorOperands,
    append_left_fill_results, append_matched_binary_result, copy_group_labels, fill_missing_right,
    index_by_match_key, labels_without_metric_name, left_sample_matches,
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
    for LeftSampleMatch {
        left: left_sample,
        key,
        right,
    } in left_sample_matches(left, &right_by_key, modifier)
    {
        let Some(right_sample) = right else {
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
        append_matched_binary_result(
            &mut out,
            &left_sample,
            right_sample,
            op,
            modifier,
            |preserves_name| {
                let mut labels = if preserves_name {
                    left_sample.labels.clone()
                } else {
                    labels_without_metric_name(&left_sample.labels)
                };
                copy_group_labels(&mut labels, &right_sample.labels, group_labels);
                labels
            },
        )?;
    }
    append_left_fill_results(
        &mut out,
        right_by_key,
        &matched,
        modifier.and_then(|modifier| modifier.fill_values.lhs),
        op,
        modifier,
        |sample| {
            let mut labels = one_to_one_binary_result_labels(&sample.labels, modifier, false);
            copy_group_labels(&mut labels, &sample.labels, group_labels);
            labels
        },
    )?;
    Ok(out)
}
