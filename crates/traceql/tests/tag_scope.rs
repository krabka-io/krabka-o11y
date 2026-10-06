use assert2::assert;
use krabka_traceql::TagScope;

#[test]
fn tag_scope_names_round_trip_with_exact_spelling() {
    for (name, scope) in [
        ("resource", TagScope::Resource),
        ("span", TagScope::Span),
        ("intrinsic", TagScope::Intrinsic),
        ("event", TagScope::Event),
        ("link", TagScope::Link),
        ("instrumentation", TagScope::Instrumentation),
    ] {
        assert!(TagScope::from_name(name) == Some(scope));
        assert!(scope.as_str() == name);
    }
    for name in ["", "Span", "spans", " span", "span ", "unknown"] {
        assert!(TagScope::from_name(name).is_none(), "{name}");
    }
}
