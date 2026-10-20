use super::StatusCode;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WireSpanStatus {
    pub(crate) code: StatusCode,
    pub(crate) message: String,
}
