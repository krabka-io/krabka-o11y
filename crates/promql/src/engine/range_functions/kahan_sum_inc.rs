/// Does one Kahan-compensated incremental sum step.
///
/// This function adds `increment` to the running sum `(sum, comp)` and returns
/// the updated `(sum, comp)`. It is a direct port of Prometheus' `kahanSumInc`
/// (`promql/engine.go`). The numerically stable mean and variance folds use it,
/// so the operator and interpreter agree bit-for-bit.
pub(crate) fn kahan_sum_inc(increment: f64, sum: f64, comp: f64) -> (f64, f64) {
    let new_sum = sum + increment;
    // Recover the rounding error lost when `increment` is small relative to
    // `sum` (or vice versa), matching Prometheus' branch on magnitude.
    //
    // An infinite running sum drops the compensation instead. Without this the
    // very first infinite increment leaves `(inf - inf) + x`, which is NaN, and
    // the NaN rides `comp` all the way to the `sum + comp` at the end -- so
    // `avg_over_time` over a series holding a single +Inf returned NaN where
    // Prometheus returns +Inf. Matches the `IsInf(t, 0)` arm of `kahanSumInc`.
    let new_comp = if new_sum.is_infinite() {
        0.0
    } else if sum.abs() >= increment.abs() {
        comp + ((sum - new_sum) + increment)
    } else {
        comp + ((increment - new_sum) + sum)
    };
    (new_sum, new_comp)
}
