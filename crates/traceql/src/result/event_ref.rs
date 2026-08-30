use super::*;

/// One span event attached to a returned span.
#[derive(Clone, Debug, PartialEq)]
pub struct EventRef {
    /// How long after the span started the event fired.
    pub time_since_start: Time,
    pub name: String,
    pub attributes: Vec<(String, AttrValue)>,
}
