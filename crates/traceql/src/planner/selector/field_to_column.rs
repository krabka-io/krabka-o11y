use super::*;

pub(crate) fn field_to_column(field: &Field) -> String {
    let col = match &field.scope {
        Scope::Intrinsic(i) => match i {
            Intrinsic::Name => COL_NAME,
            Intrinsic::Duration => COL_DURATION,
            Intrinsic::Kind => COL_KIND,
            Intrinsic::Status => COL_STATUS_CODE,
            Intrinsic::StatusMessage => COL_STATUS_MESSAGE,
            Intrinsic::Id => COL_SPAN_ID,
            Intrinsic::ParentId => COL_PARENT_SPAN_ID,
            Intrinsic::TraceDuration => COL_TRACE_DURATION,
            Intrinsic::TraceRootName => COL_ROOT_SPAN_NAME,
            Intrinsic::TraceRootService => COL_ROOT_SERVICE_NAME,
            Intrinsic::TraceId => COL_TRACE_ID,
            Intrinsic::NestedSetLeft => COL_NS_LEFT,
            Intrinsic::NestedSetRight => COL_NS_RIGHT,
            Intrinsic::NestedSetParent => COL_PARENT_ID,
            Intrinsic::ChildCount => COL_CHILD_COUNT,
            Intrinsic::InstrumentationName => COL_INSTRUMENTATION_NAME,
            Intrinsic::InstrumentationVersion => COL_INSTRUMENTATION_VERSION,
            Intrinsic::EventName => COL_EVENT_NAME,
            Intrinsic::EventTimeSinceStart => COL_EVENT_TIME_SINCE_START,
            Intrinsic::LinkTraceId => COL_LINK_TRACE_ID,
            Intrinsic::LinkSpanId => COL_LINK_SPAN_ID,
        },
        Scope::Both | Scope::Resource if field.key == "service.name" => COL_ROOT_SERVICE_NAME,
        Scope::Instrumentation => {
            return format!("{ATTR_PREFIX}{INSTRUMENTATION_ATTR_PREFIX}{}", field.key);
        }
        Scope::Both
        | Scope::Span
        | Scope::Resource
        | Scope::Parent
        | Scope::Event
        | Scope::Link => {
            return format!("{ATTR_PREFIX}{}", field.key);
        }
    };
    col.to_string()
}
