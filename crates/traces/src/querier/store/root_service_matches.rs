use super::*;

pub(crate) fn root_service_matches(value: &str, matcher: &SpanMatcher) -> bool {
    nested_presence_matches(!value.is_empty(), matcher.op, &matcher.value)
        .unwrap_or_else(|| string_matches(value, matcher.op, &matcher.value))
}
