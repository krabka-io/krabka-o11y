use super::*;

#[test]
pub(crate) fn retry_backoff_doubles_and_caps() {
    for (current, want) in [
        (millis(10), millis(20)),
        (millis(300), millis(500)),
        (millis(500), millis(500)),
    ] {
        check!(next_compactor_object_store_backoff(current, millis(500)) == want);
    }
    check!(next_compactor_object_store_backoff(millis(300), millis(400)) == millis(400));
}
