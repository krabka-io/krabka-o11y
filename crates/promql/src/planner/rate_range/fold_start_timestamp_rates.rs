use num_traits::ToPrimitive;

use super::{LabeledSeries, RateUdfKind, TimeExt};
use crate::{
    functions::extrapolate::{
        InstantKind, RangeKind, RateWindow, WindowBounds, extrapolated_rate_with_starts,
        instant_delta, start_timestamp_reset,
    },
    planner::RangeWindowGrid,
};

pub(super) fn fold_start_timestamp_rates(
    series: &mut [LabeledSeries],
    windows: RangeWindowGrid,
    kind: RateUdfKind,
) {
    let RangeWindowGrid { grid, range } = windows;
    for series in series {
        let starts = series
            .samples
            .iter()
            .filter_map(|sample| sample.start_timestamp_ms.map(|start| (sample.ts_ms, start)))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut folded = Vec::new();
        let mut end = grid.start;
        loop {
            let start = end.saturating_sub(range.millis_i64());
            let first = series
                .samples
                .partition_point(|sample| sample.ts_ms <= start);
            let last = series.samples.partition_point(|sample| sample.ts_ms <= end);
            let window = &series.samples[first..last];
            if matches!(kind, RateUdfKind::Rate | RateUdfKind::Increase)
                && window.windows(2).any(|pair| {
                    let previous = pair[0].start_timestamp_ms.unwrap_or(0);
                    let current = pair[1].start_timestamp_ms.unwrap_or(0);
                    current != 0 && current < pair[0].ts_ms && current != previous
                })
            {
                crate::engine::emit_warning(format!(
                    "PromQL warning: sample has start time that overlaps with previous sample timestamp for metric {:?}",
                    series.labels.get("__name__").unwrap_or("")
                ));
            }
            let timestamps = window.iter().map(|sample| sample.ts_ms).collect::<Vec<_>>();
            let values = window.iter().map(|sample| sample.value).collect::<Vec<_>>();
            let value = match kind {
                RateUdfKind::Rate | RateUdfKind::Increase | RateUdfKind::Delta => {
                    let kind = match kind {
                        RateUdfKind::Rate => RangeKind::Rate,
                        RateUdfKind::Increase => RangeKind::Increase,
                        _ => RangeKind::Delta,
                    };
                    extrapolated_rate_with_starts(
                        RateWindow {
                            timestamps: &timestamps,
                            values: &values,
                            bounds: WindowBounds {
                                range_start_ms: start,
                                range_end_ms: end,
                            },
                            range,
                        },
                        &starts,
                        kind,
                    )
                }
                RateUdfKind::Irate | RateUdfKind::Idelta => {
                    let kind_value = if kind == RateUdfKind::Irate {
                        InstantKind::Irate
                    } else {
                        InstantKind::Idelta
                    };
                    if window.len() >= 2
                        && kind == RateUdfKind::Irate
                        && start_timestamp_reset(
                            window[window.len() - 2].start_timestamp_ms.unwrap_or(0),
                            window[window.len() - 2].ts_ms,
                            window[window.len() - 1].start_timestamp_ms.unwrap_or(0),
                            window[window.len() - 1].ts_ms,
                        )
                    {
                        let interval = (window[window.len() - 1].ts_ms
                            - window[window.len() - 2].ts_ms)
                            .to_f64()
                            .unwrap_or(0.0)
                            / 1000.0;
                        (interval > 0.0).then(|| values[values.len() - 1] / interval)
                    } else {
                        instant_delta(&timestamps, &values, kind_value)
                    }
                }
            };
            if let Some(value) = value {
                folded.push(crate::planner::TimedValue {
                    ts_ms: end,
                    value,
                    start_timestamp_ms: None,
                });
            }
            if end >= grid.end || grid.step <= 0 {
                break;
            }
            let next = end.saturating_add(grid.step);
            if next <= end || next > grid.end {
                break;
            }
            end = next;
        }
        series.samples = folded;
    }
}
