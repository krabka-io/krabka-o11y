use std::collections::BTreeMap;

use super::{RangeFn, Time};
use crate::functions::extrapolate::RangeKind;

pub(crate) fn extrapolated_rate(
    timestamps: &[i64],
    values: &[f64],
    start: i64,
    end: i64,
    range: Time,
    kind: RangeFn,
) -> Option<f64> {
    extrapolated_rate_with_starts(
        timestamps,
        values,
        &BTreeMap::new(),
        start,
        end,
        range,
        kind,
    )
}

pub(crate) fn extrapolated_rate_with_starts(
    timestamps: &[i64],
    values: &[f64],
    starts: &BTreeMap<i64, i64>,
    start: i64,
    end: i64,
    range: Time,
    kind: RangeFn,
) -> Option<f64> {
    let kind = match kind {
        RangeFn::Rate => RangeKind::Rate,
        RangeFn::Increase => RangeKind::Increase,
        RangeFn::Delta => RangeKind::Delta,
        _ => return None,
    };
    crate::functions::extrapolate::extrapolated_rate_with_starts(
        timestamps, values, starts, start, end, range, kind,
    )
}
