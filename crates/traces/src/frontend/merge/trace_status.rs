use krabka_domain_macros::EnumName;

/// The v2 by-id status.
///
/// A fully-returned trace is `COMPLETE`. A trace that exceeds the max trace
/// size is `PARTIAL`, and the response carries an explanatory message rather
/// than an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumName)]
pub enum TraceStatus {
    #[name(value = "COMPLETE")]
    Complete,
    #[name(value = "PARTIAL")]
    Partial,
}
