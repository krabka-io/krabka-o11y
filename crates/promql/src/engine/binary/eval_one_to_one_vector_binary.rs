use super::{
    BTreeSet, InstantSample, LeftSampleMatch, PromqlError, Result, VectorMatching, VectorOperands,
    append_left_fill_results, append_matched_binary_result, default_left_fill_labels,
    fill_missing_right, index_by_match_key, one_to_one_binary_result_labels,
    visit_left_sample_matches,
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
    visit_left_sample_matches(
        left,
        &right_by_key,
        modifier,
        |LeftSampleMatch {
             left: left_sample,
             key,
             right,
         }| {
            let Some(right_sample) = right else {
                out.extend(fill_missing_right(&left_sample, matching, |fill| {
                    one_to_one_binary_result_labels(
                        &left_sample.labels,
                        modifier,
                        fill.preserves_name,
                    )
                })?);
                return Ok(());
            };
            if !matched_keys.insert(key.clone()) {
                return Err(PromqlError::Exec(format!(
                    "many-to-one matching must be explicit for key `{key}`"
                )));
            }
            append_matched_binary_result(
                &mut out,
                &left_sample,
                right_sample,
                op,
                modifier,
                |preserves_name| {
                    one_to_one_binary_result_labels(&left_sample.labels, modifier, preserves_name)
                },
            )?;
            Ok(())
        },
    )?;
    append_left_fill_results(
        &mut out,
        right_by_key,
        &matched_keys,
        modifier.and_then(|modifier| modifier.fill_values.lhs),
        op,
        modifier,
        |sample| default_left_fill_labels(sample, modifier),
    )?;
    Ok(out)
}
