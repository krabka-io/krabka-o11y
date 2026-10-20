use super::{
    InstantSample, Result, VectorMatching, VectorOperands, append_left_fill_results,
    append_left_match_results, default_left_fill_labels, index_by_match_key,
    one_to_one_binary_result_labels,
};

pub(crate) fn eval_one_to_one_vector_binary(
    operands: VectorOperands,
    matching: VectorMatching<'_>,
) -> Result<Vec<InstantSample>> {
    let VectorOperands { left, right } = operands;
    let modifier = matching.modifier;
    let right_by_key = index_by_match_key(right, modifier, |key| {
        format!("many-to-one matching for key `{key}` is not supported")
    })?;

    let mut left_matches = append_left_match_results(
        left,
        &right_by_key,
        matching,
        true,
        |left, preserves_name| {
            one_to_one_binary_result_labels(&left.labels, modifier, preserves_name)
        },
        |left, _right, preserves_name| {
            one_to_one_binary_result_labels(&left.labels, modifier, preserves_name)
        },
    )?;
    append_left_fill_results(
        &mut left_matches.samples,
        right_by_key,
        &left_matches.matched_keys,
        modifier.and_then(|modifier| modifier.fill_values.lhs),
        matching.op,
        modifier,
        |sample| default_left_fill_labels(sample, modifier),
    )?;
    Ok(left_matches.samples)
}
