use krabka_domain_macros::EnumName;

/// Prometheus 3.x range/vector selector modifier accepted after selectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumName)]
#[enum_name(accessor = "keyword")]
pub enum ExtendedSelectorModifier {
    #[name(value = "anchored")]
    Anchored,
    #[name(value = "smoothed")]
    Smoothed,
}
