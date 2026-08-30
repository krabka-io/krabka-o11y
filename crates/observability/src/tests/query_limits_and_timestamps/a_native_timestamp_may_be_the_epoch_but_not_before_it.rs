use super::*;

/// `validate_native_timestamp_ns` refuses a negative timestamp and returns
/// the value otherwise. Zero is the boundary -- the Unix epoch is a real
/// instant, so it is accepted, which is what separates `< 0` from `<= 0`.
#[test]
pub(crate) fn a_native_timestamp_may_be_the_epoch_but_not_before_it() {
    let validate = |timestamp_ns| {
        super::super::prelude::validate_native_timestamp_ns(timestamp_ns, timestamp_ns.to_string())
    };

    check!(validate(0).ok() == Some(0), "the epoch is a real instant");
    check!(validate(1).ok() == Some(1));
    check!(validate(i64::MAX).ok() == Some(i64::MAX));
    check!(validate(-1).is_err());
    check!(validate(i64::MIN).is_err());

    // The refusal carries the value it refused, so a log line names the
    // timestamp that was wrong rather than only that one was.
    let error = validate(-42).expect_err("negative is refused");
    check!(error.to_string().contains("-42"), "got: {error}");
}
