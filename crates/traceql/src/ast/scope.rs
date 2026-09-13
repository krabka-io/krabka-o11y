use super::Intrinsic;

#[derive(Clone, Debug, PartialEq, Eq)]
/// The `TraceQL` scope used to resolve a field.
pub enum Scope {
    Both,
    Span,
    Resource,
    Parent,
    Event,
    Link,
    Instrumentation,
    Intrinsic(Intrinsic),
}
