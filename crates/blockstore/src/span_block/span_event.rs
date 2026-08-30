use super::*;

/// One nested span event.
///
/// `time_since_start` is an offset from the owning span's start, so it is an
/// extent and not an instant. The span's start itself stays a raw stamp.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanEvent {
    pub name: String,
    pub time_since_start: Time,
    pub attrs: Vec<(String, String)>,
}
