use super::*;

/// One expected float point of a range-vector series.
#[derive(Clone, Copy)]
pub(crate) struct ExpectedPoint {
    pub(crate) ts_ms: i64,
    pub(crate) value: f64,
}

/// Returns the only series of a range-matrix `result`.
pub(crate) fn lone_matrix_series(result: &QueryResult) -> &crate::RangeSeries {
    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix");
    };
    check!(series.len() == 1);
    &series[0]
}

/// Checks that `series` holds exactly `points`, each value approximately.
pub(crate) fn check_series_points(series: &crate::RangeSeries, points: &[ExpectedPoint]) {
    check!(series.samples.len() == points.len());
    for (sample, point) in series.samples.iter().zip(points) {
        let want_ts = point.ts_ms;
        check!(sample.0 == want_ts);
        check!(
            approx_eq(float_value(&sample.1), point.value),
            "at ts {want_ts}"
        );
    }
}
