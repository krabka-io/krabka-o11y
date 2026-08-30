use super::TagScope;

pub(crate) const SCOPE_ORDER: &[TagScope] = &[
    TagScope::Resource,
    TagScope::Span,
    TagScope::Intrinsic,
    TagScope::Event,
    TagScope::Link,
    TagScope::Instrumentation,
];
