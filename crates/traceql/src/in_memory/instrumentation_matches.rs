use super::*;

pub(crate) fn instrumentation_matches(span: &InputSpan, matcher: &SpanMatcher) -> bool {
    match matcher.key.as_str() {
        "name" | "instrumentation:name" => {
            string_matches(&span.instrumentation_name, matcher.op, &matcher.value)
        }
        "version" | "instrumentation:version" => {
            string_matches(&span.instrumentation_version, matcher.op, &matcher.value)
        }
        _ => span_attr_matches(
            span,
            &format!("{}{}", crate::INSTRUMENTATION_ATTR_PREFIX, matcher.key),
            matcher.op,
            &matcher.value,
        ),
    }
}
