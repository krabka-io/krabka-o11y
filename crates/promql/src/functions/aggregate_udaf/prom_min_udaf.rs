use super::{AggregateUDF, Extremum, extremum_udaf};

/// The NaN-ignoring `min` aggregate UDAF.
#[must_use]
pub fn prom_min_udaf() -> AggregateUDF {
    extremum_udaf(Extremum::Min)
}
