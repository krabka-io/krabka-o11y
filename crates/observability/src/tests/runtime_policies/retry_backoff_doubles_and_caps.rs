use super::*;

struct BackoffCase {
    current: krabka_units::Time,
    expected: krabka_units::Time,
}

#[test]
pub(crate) fn retry_backoff_doubles_and_caps() {
    for case in [
        BackoffCase {
            current: millis(10),
            expected: millis(20),
        },
        BackoffCase {
            current: millis(300),
            expected: millis(500),
        },
        BackoffCase {
            current: millis(500),
            expected: millis(500),
        },
    ] {
        check!(next_compactor_object_store_backoff(case.current, millis(500)) == case.expected);
    }
    check!(next_compactor_object_store_backoff(millis(300), millis(400)) == millis(400));
}
