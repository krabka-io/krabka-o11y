use super::*;

/// Wraps a scalar `QueryResult` from a delegated interpreter call.
///
/// The result becomes a `PlannedInstant::PrecomputedScalar`. A non-scalar result
/// is impossible for these callers. This function still maps such a result to a
/// canonical error instead of a panic.
#[cfg(feature = "experimental-functions")]
pub(crate) fn scalar_call_to_planned(result: &QueryResult) -> Result<PlannedInstant> {
    match *result {
        QueryResult::Scalar { ts_ms, value } => {
            Ok(PlannedInstant::PrecomputedScalar { ts_ms, value })
        }
        _ => Err(PromqlError::Plan(
            "expected a scalar result from an experimental scalar call".to_string(),
        )),
    }
}
