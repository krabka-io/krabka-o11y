use super::{
    BinModifier, BinaryOp, InstantSample, MissingSide, Result, SampleValue,
    apply_binary_fill_value, binary_returns_bool,
};

/// A left-side sample's result against the `fill_right` value that stands in
/// for its missing right-side match.
pub(crate) struct RightFill {
    pub(crate) filled_value: SampleValue,
    /// Whether the operation keeps the left labels, metric name included.
    pub(crate) preserves_name: bool,
    pub(crate) drop_name: bool,
}

/// Applies the `fill_right` modifier value to a left-side sample that found no
/// right-side match. `None` means no fill applies, or the filled comparison
/// filtered the sample out.
pub(crate) fn fill_missing_right(
    left_sample: &InstantSample,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Option<RightFill>> {
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
    Ok(Some(RightFill {
        filled_value: filled,
        preserves_name,
        drop_name,
    }))
}
