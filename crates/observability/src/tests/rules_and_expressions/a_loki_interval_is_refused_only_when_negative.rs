use super::*;

/// `validate_loki_interval` refuses a negative step and accepts everything
/// else, including zero and an absent one. Zero is the boundary that
/// separates `< 0` from `<= 0`, and an absent interval is not the same as
/// a zero one -- absent means the caller did not ask.
#[test]
pub(crate) fn a_loki_interval_is_refused_only_when_negative() {
    let validate = super::super::prelude::validate_loki_interval;

    check!(validate(None).is_ok(), "an absent interval is not an error");
    check!(validate(Some(0)).is_ok(), "and neither is zero");
    check!(validate(Some(1)).is_ok());
    check!(validate(Some(i64::MAX)).is_ok());
    check!(matches!(
        validate(Some(-1)),
        Err(HttpQueryError::InvalidInterval)
    ));
    check!(validate(Some(i64::MIN)).is_err());
}
