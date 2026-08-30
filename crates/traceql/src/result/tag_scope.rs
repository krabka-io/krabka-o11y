use super::*;

/// Tag discovery scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TagScope {
    Resource,
    Span,
    Intrinsic,
    Event,
    Link,
    Instrumentation,
}
