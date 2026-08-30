use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
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
