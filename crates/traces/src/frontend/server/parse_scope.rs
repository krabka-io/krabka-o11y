
pub(crate) fn parse_scope(name: &str) -> Option<krabka_traceql::TagScope> {
    Some(match name {
        "resource" => krabka_traceql::TagScope::Resource,
        "span" => krabka_traceql::TagScope::Span,
        "intrinsic" => krabka_traceql::TagScope::Intrinsic,
        "event" => krabka_traceql::TagScope::Event,
        "link" => krabka_traceql::TagScope::Link,
        "instrumentation" => krabka_traceql::TagScope::Instrumentation,
        _ => return None,
    })
}
