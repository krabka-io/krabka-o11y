/// Does one Kahan-compensated incremental sum step.
///
/// This function is a port of Prometheus' `kahanSumInc` (`promql/engine.go`).
/// It returns the updated `(sum, comp)` after it adds `increment`. The mean and
/// variance folds then agree bit-for-bit with the engine.
pub(crate) fn kahan_sum_inc(increment: f64, sum: f64, comp: f64) -> (f64, f64) {
    let new_sum = sum + increment;
    // An infinite running sum drops the compensation instead of recovering it:
    // `(inf - inf) + x` is NaN, and that NaN would ride `comp` to the final
    // `sum + comp`, turning an infinite mean into a NaN. Matches the
    // `IsInf(t, 0)` arm of Prometheus' `kahanSumInc`.
    let new_comp = if new_sum.is_infinite() {
        0.0
    } else if sum.abs() >= increment.abs() {
        comp + ((sum - new_sum) + increment)
    } else {
        comp + ((increment - new_sum) + sum)
    };
    (new_sum, new_comp)
}
