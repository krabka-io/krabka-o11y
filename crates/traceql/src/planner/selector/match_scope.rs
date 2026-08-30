use super::*;

pub(crate) fn match_scope(scope: &Scope) -> MatchScope {
    match scope {
        Scope::Both => MatchScope::Both,
        Scope::Span => MatchScope::Span,
        Scope::Resource => MatchScope::Resource,
        Scope::Parent => MatchScope::Parent,
        Scope::Event => MatchScope::Event,
        Scope::Link => MatchScope::Link,
        Scope::Instrumentation => MatchScope::Instrumentation,
        Scope::Intrinsic(_) => MatchScope::Intrinsic,
    }
}
