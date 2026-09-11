use std::any::Any;

/// The message a panic carried, for a log line.
///
/// `panic!` stores a `&'static str` when it has no arguments and a `String`
/// when it has. A panic raised by any other means carries a payload this
/// cannot read, which is rare enough to name rather than to decode.
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message
    } else {
        "panic payload of an unknown type"
    }
}
