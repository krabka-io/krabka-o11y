use super::*;

/// `ScalarSample` holds a rational, and its division normalises the sign
/// so the denominator stays positive -- a negative divisor moves its sign
/// to the numerator rather than leaving the pair in a form the rest of the
/// type does not expect. Both signs are checked on each side.
///
/// Division and power also refuse rather than produce a nonsense value:
/// dividing by zero has no answer, and a negative base to a fractional
/// power is NaN, which must not reach a series as a sample.
#[test]
pub(crate) fn scalar_division_and_power_refuse_what_has_no_answer() {
    let scalar = super::super::prelude::ScalarSample::new;
    let value = |result: Option<super::super::prelude::ScalarSample>| {
        result.and_then(super::super::prelude::ScalarSample::to_f64)
    };

    // Exact division, and a repeating fraction held as a rational rather
    // than rounded on the way in.
    check!(value(scalar(6, 1).divide(scalar(3, 1))) == Some(2.0));
    check!(value(scalar(1, 1).divide(scalar(3, 1))) == Some(1.0 / 3.0));
    check!(value(scalar(0, 1).divide(scalar(5, 1))) == Some(0.0));

    // Sign normalisation: a negative divisor, a negative dividend, and
    // both. Only the last returns to positive.
    check!(value(scalar(4, 1).divide(scalar(-2, 1))) == Some(-2.0));
    check!(value(scalar(-4, 1).divide(scalar(2, 1))) == Some(-2.0));
    check!(value(scalar(-4, 1).divide(scalar(-2, 1))) == Some(2.0));

    // Dividing by zero has no answer, whatever the dividend.
    check!(scalar(1, 1).divide(scalar(0, 1)).is_none());
    check!(scalar(0, 1).divide(scalar(0, 1)).is_none());
    check!(scalar(-1, 1).divide(scalar(0, 1)).is_none());

    // Powers, including the ones that are easy to get backwards.
    check!(value(scalar(2, 1).power(scalar(3, 1))) == Some(8.0));
    check!(
        value(scalar(3, 1).power(scalar(2, 1))) == Some(9.0),
        "not the other way round"
    );
    check!(value(scalar(2, 1).power(scalar(-1, 1))) == Some(0.5));
    check!(
        value(scalar(4, 1).power(scalar(1, 2))) == Some(2.0),
        "a fractional exponent"
    );
    check!(value(scalar(5, 1).power(scalar(0, 1))) == Some(1.0));

    // A negative base to a fractional power is NaN, which must be refused
    // rather than carried into a series as a sample.
    check!(scalar(-4, 1).power(scalar(1, 2)).is_none());
}
