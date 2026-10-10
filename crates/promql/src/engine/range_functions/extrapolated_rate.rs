use std::collections::BTreeMap;

use super::RangeFn;
use crate::functions::extrapolate::{RangeKind, RateWindow};

pub(crate) fn extrapolated_rate(window: RateWindow<'_>, kind: RangeFn) -> Option<f64> {
    extrapolated_rate_with_starts(window, &BTreeMap::new(), kind)
}

pub(crate) fn extrapolated_rate_with_starts(
    window: RateWindow<'_>,
    starts: &BTreeMap<i64, i64>,
    kind: RangeFn,
) -> Option<f64> {
    let kind = match kind {
        RangeFn::Rate => RangeKind::Rate,
        RangeFn::Increase => RangeKind::Increase,
        RangeFn::Delta => RangeKind::Delta,
        _ => return None,
    };
    crate::functions::extrapolate::extrapolated_rate_with_starts(window, starts, kind)
}
