use krabka_domain_macros::EnumName;

/// Tag discovery scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumName)]
#[enum_name(parse)]
pub enum TagScope {
    #[name(value = "resource")]
    Resource,
    #[name(value = "span")]
    Span,
    #[name(value = "intrinsic")]
    Intrinsic,
    #[name(value = "event")]
    Event,
    #[name(value = "link")]
    Link,
    #[name(value = "instrumentation")]
    Instrumentation,
}
