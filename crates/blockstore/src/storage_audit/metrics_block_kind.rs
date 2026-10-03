use super::Deserialize;

/// The block kind of a metrics `.index` manifest, as `krabka-metrics`
/// persists it in `MetricBlockKind`.
///
/// The manifest codec writes a variant as its index, so the variants keep
/// the order of that enum. An index outside it does not decode.
#[derive(Clone, Copy, Debug, Deserialize)]
pub enum MetricsBlockKind {
    Float,
    NativeHistograms,
    Exemplars,
    Metadata,
    ClockReadings,
}
