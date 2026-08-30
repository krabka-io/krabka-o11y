use super::{IrateFn, OuterRangeFn, RangeFn, RateUdfKind};

/// Maps a [`RateUdfKind`] to the shared [`OuterRangeFn`] of the same name.
///
/// [`RateUdfKind`] is the output of the rate-family matcher, and the
/// `eval_*_call` of the interpreter applies the [`OuterRangeFn`].
/// `rate`/`increase`/`delta` are extrapolated range folds. `irate`/`idelta` are
/// instant-delta folds. This is the seam that lets a histogram-bearing
/// rate-family call route through the shared `apply_outer_range_fn` kernel
/// instead of the float-only UDF chain.
pub(crate) fn rate_udf_kind_to_outer_range_fn(kind: RateUdfKind) -> OuterRangeFn {
    match kind {
        RateUdfKind::Rate => OuterRangeFn::Range(RangeFn::Rate),
        RateUdfKind::Increase => OuterRangeFn::Range(RangeFn::Increase),
        RateUdfKind::Delta => OuterRangeFn::Range(RangeFn::Delta),
        RateUdfKind::Irate => OuterRangeFn::InstantDelta(IrateFn::Irate),
        RateUdfKind::Idelta => OuterRangeFn::InstantDelta(IrateFn::Idelta),
    }
}
