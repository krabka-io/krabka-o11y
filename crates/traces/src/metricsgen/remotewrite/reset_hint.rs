use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub(crate) enum ResetHint {
    Unknown = 0,
    Yes = 1,
    No = 2,
    Gauge = 3,
}
