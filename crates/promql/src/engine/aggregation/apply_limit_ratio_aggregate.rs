
/// Shared experimental `limit_ratio(ratio, v)` core over an already-evaluated
/// instant vector.
///
/// This function backs both the interpreter
/// (`PromqlEngine::eval_limit_ratio_aggregate`) and the operator path. It keeps
/// each sample whose label-set hash falls in the ratio's deterministic selection
/// band, as [`limit_ratio_includes_sample`] defines. The caller resolves and
/// caps the ratio before reaching here, and raises the `InvalidRatioWarning`
/// when the ratio was out of range. The caller also short-circuits `ratio==0` to
/// the empty vector.
#[cfg(feature = "experimental-functions")]
pub(crate) fn apply_limit_ratio_aggregate(
    samples: Vec<InstantSample>,
    ratio: f64,
) -> Vec<InstantSample> {
    samples
        .into_iter()
        .filter(|sample| limit_ratio_includes_sample(ratio, &sample.labels))
        .collect()
}
