use super::*;

pub(crate) fn intrinsic(scope: &str, key: &str) -> Result<Intrinsic> {
    match (scope, key) {
        ("span", "name") => Ok(Intrinsic::Name),
        ("span", "duration") => Ok(Intrinsic::Duration),
        ("span", "kind") => Ok(Intrinsic::Kind),
        ("span", "status") => Ok(Intrinsic::Status),
        ("span", "statusMessage") => Ok(Intrinsic::StatusMessage),
        ("span", "id") => Ok(Intrinsic::Id),
        ("span", "parentID" | "parentId") => Ok(Intrinsic::ParentId),
        ("span", "childCount") => Ok(Intrinsic::ChildCount),
        ("trace", "duration") => Ok(Intrinsic::TraceDuration),
        ("trace", "rootName") => Ok(Intrinsic::TraceRootName),
        ("trace", "rootService") => Ok(Intrinsic::TraceRootService),
        ("trace", "id") => Ok(Intrinsic::TraceId),
        ("event", "name") => Ok(Intrinsic::EventName),
        ("event", "timeSinceStart") => Ok(Intrinsic::EventTimeSinceStart),
        ("link", "traceID" | "traceId") => Ok(Intrinsic::LinkTraceId),
        ("link", "spanID" | "spanId") => Ok(Intrinsic::LinkSpanId),
        ("instrumentation", "name") => Ok(Intrinsic::InstrumentationName),
        ("instrumentation", "version") => Ok(Intrinsic::InstrumentationVersion),
        ("span", "nestedSetLeft") => Ok(Intrinsic::NestedSetLeft),
        ("span", "nestedSetRight") => Ok(Intrinsic::NestedSetRight),
        ("span", "nestedSetParent") => Ok(Intrinsic::NestedSetParent),
        _ => Err(TraceqlError::Parse(format!(
            "unknown intrinsic {scope}:{key}"
        ))),
    }
}
