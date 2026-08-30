use super::*;

/// `MetricValue::modulo` refuses a zero divisor rather than producing a
/// NaN, which is the whole reason it is not just `%`.
#[test]
pub(crate) fn metric_modulo_refuses_a_zero_divisor() {
    use super::super::prelude::MetricValue;

    let modulo = |a: f64, b: f64| {
        MetricValue::from_f64(a)?
            .modulo(MetricValue::from_f64(b)?)
            .and_then(super::super::prelude::MetricValue::to_f64)
    };

    check!(modulo(7.0, 3.0) == Some(1.0));
    check!(modulo(7.5, 2.5) == Some(0.0));
    check!(
        modulo(-7.0, 3.0) == Some(-1.0),
        "the sign follows the dividend"
    );
    check!(
        modulo(3.0, 7.0) == Some(3.0),
        "a smaller dividend is itself"
    );
    check!(modulo(1.0, 0.0) == None, "a zero divisor has no answer");
    check!(modulo(0.0, 3.0) == Some(0.0), "but a zero dividend does");
}
