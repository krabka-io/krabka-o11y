/// `kahan_sum_inc` recovers the bits lost when one operand dwarfs the other,
/// on both magnitude branches, and gives up on an infinite running sum rather
/// than carrying a NaN forward.
#[test]
pub(crate) fn kahan_sum_inc_recovers_lost_bits_on_both_branches() {
    use super::super::range_functions::kahan_sum_inc;

    // |sum| >= |increment|: the increment falls off the end of the sum, and
    // all of it comes back as compensation.
    let mut sum = 1e16;
    let mut comp = 0.0;
    kahan_sum_inc(1.0, &mut sum, &mut comp);
    assert2::assert!(sum.to_bits() == 1e16_f64.to_bits());
    assert2::assert!(comp.to_bits() == 1.0_f64.to_bits(), "got {comp}");

    // The swapped branch has to compute the residue the other way round.
    let mut sum = 1.0;
    let mut comp = 0.0;
    kahan_sum_inc(1e16, &mut sum, &mut comp);
    assert2::assert!(sum.to_bits() == 1e16_f64.to_bits());
    assert2::assert!(comp.to_bits() == 1.0_f64.to_bits(), "got {comp}");

    // An infinite sum leaves no residue to recover; computing one gives NaN.
    let mut sum = 0.0;
    let mut comp = 0.0;
    kahan_sum_inc(f64::INFINITY, &mut sum, &mut comp);
    assert2::assert!(sum.to_bits() == f64::INFINITY.to_bits());
    assert2::assert!(comp.to_bits() == 0.0_f64.to_bits(), "got {comp}");
}
