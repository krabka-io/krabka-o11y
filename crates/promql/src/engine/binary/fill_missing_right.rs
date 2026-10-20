use super::{
    InstantSample, Labels, MissingSide, Result, VectorMatching, apply_binary_fill_value,
    binary_returns_bool,
};

/// How the `fill_right` value treats the metric name of the left-side sample
/// it stands in against.
pub(crate) struct RightFill {
    /// Whether the operation keeps the left labels, metric name included.
    pub(crate) preserves_name: bool,
}

/// Applies the `fill_right` modifier value to a left-side sample that found no
/// right-side match, labelling the result with `result_labels`. `None` means
/// no fill applies, or the filled comparison filtered the sample out.
pub(crate) fn fill_missing_right(
    left_sample: &InstantSample,
    matching: VectorMatching<'_>,
    result_labels: impl FnOnce(&RightFill) -> Labels,
) -> Result<Option<InstantSample>> {
    let VectorMatching { op, modifier } = matching;
    let Some(rhs_fill) = modifier.and_then(|modifier| modifier.fill_values.rhs) else {
        return Ok(None);
    };
    let Some(filled) =
        apply_binary_fill_value(left_sample, rhs_fill, op, modifier, MissingSide::Right)?
    else {
        return Ok(None);
    };
    let preserves_name = op.is_comparison() && !binary_returns_bool(modifier);
    let drop_name = if preserves_name {
        left_sample.drop_name
    } else {
        true
    };
    Ok(Some(InstantSample {
        labels: result_labels(&RightFill { preserves_name }),
        ts_ms: left_sample.ts_ms,
        value: filled,
        drop_name,
    }))
}
