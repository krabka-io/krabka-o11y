use super::*;

/// Resolves a bare identifier with no scope to a `TraceQL` intrinsic.
///
/// The set matches the reserved intrinsic field names that Tempo recognizes
/// without a scope. A name outside this set is a span or resource attribute,
/// that is, `Scope::Both`. This function excludes `parentID`, `id`, `traceID`,
/// and the event, link, and instrumentation intrinsics on purpose. Tempo needs
/// an explicit scope such as `span:` or `trace:` for those names.
pub(crate) fn scopeless_intrinsic(name: &str) -> Option<Intrinsic> {
    Some(match name {
        "duration" => Intrinsic::Duration,
        "kind" => Intrinsic::Kind,
        "name" => Intrinsic::Name,
        "status" => Intrinsic::Status,
        "statusMessage" => Intrinsic::StatusMessage,
        "childCount" => Intrinsic::ChildCount,
        "nestedSetLeft" => Intrinsic::NestedSetLeft,
        "nestedSetRight" => Intrinsic::NestedSetRight,
        "nestedSetParent" => Intrinsic::NestedSetParent,
        "rootName" => Intrinsic::TraceRootName,
        "rootServiceName" => Intrinsic::TraceRootService,
        "traceDuration" => Intrinsic::TraceDuration,
        _ => return None,
    })
}
