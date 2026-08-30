use super::*;

pub(crate) fn metric_field_column(field: &Field) -> Result<String> {
    match field.scope {
        Scope::Both | Scope::Resource if field.key == "service.name" => {
            Ok(COL_ROOT_SERVICE_NAME.to_string())
        }
        Scope::Both | Scope::Span | Scope::Resource => Ok(format!("{ATTR_PREFIX}{}", field.key)),
        Scope::Event => Ok(format!("{ATTR_PREFIX}{EVENT_ATTR_PREFIX}{}", field.key)),
        Scope::Link => Ok(format!("{ATTR_PREFIX}{LINK_ATTR_PREFIX}{}", field.key)),
        Scope::Instrumentation => Ok(format!(
            "{ATTR_PREFIX}{INSTRUMENTATION_ATTR_PREFIX}{}",
            field.key
        )),
        Scope::Intrinsic(Intrinsic::Name) => Ok(COL_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::Duration) => Ok(COL_DURATION.to_string()),
        Scope::Intrinsic(Intrinsic::Id) => Ok(COL_SPAN_ID.to_string()),
        Scope::Intrinsic(Intrinsic::ParentId) => Ok(COL_PARENT_SPAN_ID.to_string()),
        Scope::Intrinsic(Intrinsic::ChildCount) => Ok(COL_CHILD_COUNT.to_string()),
        Scope::Intrinsic(Intrinsic::NestedSetLeft) => Ok(COL_NS_LEFT.to_string()),
        Scope::Intrinsic(Intrinsic::NestedSetRight) => Ok(COL_NS_RIGHT.to_string()),
        Scope::Intrinsic(Intrinsic::NestedSetParent) => Ok(COL_PARENT_ID.to_string()),
        Scope::Intrinsic(Intrinsic::Kind) => Ok(COL_KIND.to_string()),
        Scope::Intrinsic(Intrinsic::Status) => Ok(COL_STATUS_CODE.to_string()),
        Scope::Intrinsic(Intrinsic::StatusMessage) => Ok(COL_STATUS_MESSAGE.to_string()),
        Scope::Intrinsic(Intrinsic::TraceId) => Ok(COL_TRACE_ID.to_string()),
        Scope::Intrinsic(Intrinsic::TraceDuration) => Ok(COL_TRACE_DURATION.to_string()),
        Scope::Intrinsic(Intrinsic::TraceRootService) => Ok(COL_ROOT_SERVICE_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::TraceRootName) => Ok(COL_ROOT_SPAN_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::InstrumentationName) => {
            Ok(COL_INSTRUMENTATION_NAME.to_string())
        }
        Scope::Intrinsic(Intrinsic::InstrumentationVersion) => {
            Ok(COL_INSTRUMENTATION_VERSION.to_string())
        }
        Scope::Intrinsic(Intrinsic::EventName) => Ok(COL_EVENT_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::EventTimeSinceStart) => {
            Ok(COL_EVENT_TIME_SINCE_START.to_string())
        }
        Scope::Intrinsic(Intrinsic::LinkTraceId) => Ok(COL_LINK_TRACE_ID.to_string()),
        Scope::Intrinsic(Intrinsic::LinkSpanId) => Ok(COL_LINK_SPAN_ID.to_string()),
        _ => Err(TraceqlError::Unsupported(format!(
            "metrics by() field {field:?} is not supported yet"
        ))),
    }
}
