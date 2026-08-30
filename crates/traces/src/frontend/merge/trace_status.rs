use super::*;

/// The v2 by-id status.
///
/// A fully-returned trace is `COMPLETE`. A trace that exceeds the max trace
/// size is `PARTIAL`, and the response carries an explanatory message rather
/// than an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceStatus {
    Complete,
    Partial,
}

impl TraceStatus {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            TraceStatus::Complete => "COMPLETE",
            TraceStatus::Partial => "PARTIAL",
        }
    }
}
