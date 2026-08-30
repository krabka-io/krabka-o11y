pub(crate) fn scope_param(scope: krabka_traceql::TagScope) -> &'static str {
    match scope {
        krabka_traceql::TagScope::Resource => "resource",
        krabka_traceql::TagScope::Span => "span",
        krabka_traceql::TagScope::Intrinsic => "intrinsic",
        krabka_traceql::TagScope::Event => "event",
        krabka_traceql::TagScope::Link => "link",
        krabka_traceql::TagScope::Instrumentation => "instrumentation",
    }
}
