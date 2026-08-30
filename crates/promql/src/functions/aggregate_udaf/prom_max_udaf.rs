use super::{AggregateUDF, Extremum, extremum_udaf};

/// The NaN-ignoring `max` aggregate UDAF.
#[must_use]
pub fn prom_max_udaf() -> AggregateUDF {
    extremum_udaf(Extremum::Max)
}
