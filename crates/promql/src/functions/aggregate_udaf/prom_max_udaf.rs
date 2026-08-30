use super::{AggregateUDF, extremum_udaf, Extremum};

/// The NaN-ignoring `max` aggregate UDAF.
#[must_use]
pub fn prom_max_udaf() -> AggregateUDF {
    extremum_udaf(Extremum::Max)
}
