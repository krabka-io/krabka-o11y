
pub(crate) fn parse_scope(name: &str) -> krabka_traceql::TagScope {
    match name {
        "resource" => krabka_traceql::TagScope::Resource,
        "intrinsic" => krabka_traceql::TagScope::Intrinsic,
        "event" => krabka_traceql::TagScope::Event,
        "link" => krabka_traceql::TagScope::Link,
        "instrumentation" => krabka_traceql::TagScope::Instrumentation,
        _ => krabka_traceql::TagScope::Span,
    }
}
