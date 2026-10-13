use super::{Intrinsic, Result, TraceqlError};

pub(crate) fn intrinsic(scope: &str, key: &str) -> Result<Intrinsic> {
    match scope {
        "span" => match key {
            "name" => Ok(Intrinsic::Name),
            "duration" => Ok(Intrinsic::Duration),
            "kind" => Ok(Intrinsic::Kind),
            "status" => Ok(Intrinsic::Status),
            "statusMessage" => Ok(Intrinsic::StatusMessage),
            "id" => Ok(Intrinsic::Id),
            "parentID" | "parentId" => Ok(Intrinsic::ParentId),
            "childCount" => Ok(Intrinsic::ChildCount),
            "nestedSetLeft" => Ok(Intrinsic::NestedSetLeft),
            "nestedSetRight" => Ok(Intrinsic::NestedSetRight),
            "nestedSetParent" => Ok(Intrinsic::NestedSetParent),
            _ => return unknown_intrinsic(scope, key),
        },
        "trace" => match key {
            "duration" => Ok(Intrinsic::TraceDuration),
            "rootName" => Ok(Intrinsic::TraceRootName),
            "rootService" => Ok(Intrinsic::TraceRootService),
            "id" => Ok(Intrinsic::TraceId),
            _ => return unknown_intrinsic(scope, key),
        },
        "event" => match key {
            "name" => Ok(Intrinsic::EventName),
            "timeSinceStart" => Ok(Intrinsic::EventTimeSinceStart),
            _ => return unknown_intrinsic(scope, key),
        },
        "link" => match key {
            "traceID" | "traceId" => Ok(Intrinsic::LinkTraceId),
            "spanID" | "spanId" => Ok(Intrinsic::LinkSpanId),
            _ => return unknown_intrinsic(scope, key),
        },
        "instrumentation" => match key {
            "name" => Ok(Intrinsic::InstrumentationName),
            "version" => Ok(Intrinsic::InstrumentationVersion),
            _ => return unknown_intrinsic(scope, key),
        },
        _ => Err(TraceqlError::Parse(format!(
            "unknown intrinsic {scope}:{key}"
        ))),
    }
}

fn unknown_intrinsic(scope: &str, key: &str) -> Result<Intrinsic> {
    Err(TraceqlError::Parse(format!(
        "unknown intrinsic {scope}:{key}"
    )))
}
