
/// Prometheus translation strategy for OTLP metric names.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TranslationStrategy {
    /// Replaces unsupported Prometheus metric and label characters with
    /// underscores, and applies the conventional suffixes such as `_total` for
    /// monotonic sums.
    #[default]
    UnderscoreEscapingWithSuffixes,
}
