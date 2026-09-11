use std::panic::AssertUnwindSafe;

use assert2::assert;

use super::{PanicSafeShared, panic_message};

#[test]
fn a_panic_message_is_read_from_either_payload_shape() {
    let literal =
        std::panic::catch_unwind(|| panic!("no arguments")).expect_err("the closure panicked");
    let formatted =
        std::panic::catch_unwind(|| panic!("{} arguments", 1)).expect_err("the closure panicked");
    let other = std::panic::catch_unwind(|| std::panic::panic_any(7_u32))
        .expect_err("the closure panicked");

    assert!(panic_message(literal.as_ref()) == "no arguments");
    assert!(panic_message(formatted.as_ref()) == "1 arguments");
    assert!(panic_message(other.as_ref()) == "panic payload of an unknown type");
}

/// The failure this type exists for: a panic inside an update must not leave
/// the shared value half-written, and must not stop the next reader.
#[test]
fn a_panic_during_an_update_leaves_the_previous_value_readable() {
    let shared: PanicSafeShared<Vec<String>> = PanicSafeShared::default();
    shared.update(|lines| lines.push("first".to_string()));

    let caught = std::panic::catch_unwind(AssertUnwindSafe(|| {
        shared.update(|lines| {
            lines.push("half".to_string());
            panic!("one bad record");
        });
    }));

    assert!(caught.is_err());
    assert!(shared.snapshot() == vec!["first".to_string()]);
}

/// And the state stays writable afterwards: a poisoned lock that keeps a
/// consistent value is a lock that still works.
#[test]
fn the_shared_value_is_still_writable_after_a_panic() {
    let shared: PanicSafeShared<Vec<String>> = PanicSafeShared::default();
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        shared.update(|_| panic!("one bad record"));
    }));

    shared.update(|lines| lines.push("after".to_string()));

    assert!(shared.snapshot() == vec!["after".to_string()]);
}

/// A panic inside a read mutates nothing, so the next reader sees the value
/// rather than an `expect` on the poison flag.
#[test]
fn a_panic_during_a_read_leaves_the_value_readable() {
    let shared = PanicSafeShared::new(7_u32);
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
        shared.read(|_| panic!("one bad query"));
    }));

    assert!(shared.read(|value| *value) == 7);
}
