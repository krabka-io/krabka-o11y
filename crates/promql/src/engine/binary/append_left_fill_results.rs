use super::{
    BTreeMap, BTreeSet, BinModifier, BinaryOp, InstantSample, MissingSide, Result,
    apply_binary_fill_value, apply_binary_sample_value, binary_returns_bool,
    one_to_one_binary_result_labels,
};

pub(crate) fn append_matched_binary_result(
    output: &mut Vec<InstantSample>,
    left: &InstantSample,
    right: &InstantSample,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
    result_labels: impl FnOnce(bool) -> super::Labels,
) -> Result<()> {
    let Some(value) = apply_binary_sample_value(left, right, op, modifier)? else {
        return Ok(());
    };
    let preserves_name = op.is_comparison() && !binary_returns_bool(modifier);
    output.push(InstantSample {
        labels: result_labels(preserves_name),
        ts_ms: left.ts_ms,
        value,
        drop_name: !preserves_name || left.drop_name,
    });
    Ok(())
}

pub(crate) fn append_left_fill_results(
    output: &mut Vec<InstantSample>,
    candidates: BTreeMap<String, InstantSample>,
    matched_keys: &BTreeSet<String>,
    fill_value: Option<f64>,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
    mut result_labels: impl FnMut(&InstantSample) -> super::Labels,
) -> Result<()> {
    let Some(fill_value) = fill_value else {
        return Ok(());
    };

    for (key, sample) in candidates {
        if matched_keys.contains(&key) {
            continue;
        }
        let Some(value) =
            apply_binary_fill_value(&sample, fill_value, op, modifier, MissingSide::Left)?
        else {
            continue;
        };
        output.push(InstantSample {
            labels: result_labels(&sample),
            ts_ms: sample.ts_ms,
            value,
            drop_name: true,
        });
    }
    Ok(())
}

pub(super) fn default_left_fill_labels(
    sample: &InstantSample,
    modifier: Option<&BinModifier>,
) -> super::Labels {
    one_to_one_binary_result_labels(&sample.labels, modifier, false)
}
