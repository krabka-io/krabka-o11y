/// Whether two planner outputs agree within `1e-9`.
pub(crate) fn approx_eq(left: f64, right: f64) -> bool {
    (left - right).abs() < 1e-9
}
