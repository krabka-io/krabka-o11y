use super::Intrinsic;

pub(crate) fn intrinsic_tag_name(intrinsic: &Intrinsic) -> &'static str {
    match intrinsic {
        Intrinsic::Name => "span:name",
        Intrinsic::Duration => "span:duration",
        Intrinsic::Kind => "span:kind",
        Intrinsic::Status => "span:status",
        Intrinsic::StatusMessage => "span:statusMessage",
        Intrinsic::Id => "span:id",
        Intrinsic::ParentId => "span:parentID",
        Intrinsic::ChildCount => "span:childCount",
        Intrinsic::TraceDuration => "trace:duration",
        Intrinsic::TraceRootName => "trace:rootName",
        Intrinsic::TraceRootService => "trace:rootService",
        Intrinsic::TraceId => "trace:id",
        Intrinsic::EventName => "event:name",
        Intrinsic::EventTimeSinceStart => "event:timeSinceStart",
        Intrinsic::LinkTraceId => "link:traceID",
        Intrinsic::LinkSpanId => "link:spanID",
        Intrinsic::InstrumentationName => "instrumentation:name",
        Intrinsic::InstrumentationVersion => "instrumentation:version",
        Intrinsic::NestedSetLeft => "span:nestedSetLeft",
        Intrinsic::NestedSetRight => "span:nestedSetRight",
        Intrinsic::NestedSetParent => "span:nestedSetParent",
    }
}
