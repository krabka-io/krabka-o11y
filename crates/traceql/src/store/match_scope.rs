#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchScope {
    Both,
    Span,
    Resource,
    Intrinsic,
    Parent,
    Event,
    Link,
    Instrumentation,
}
