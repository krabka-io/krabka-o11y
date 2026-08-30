use super::*;

/// The `increase` UDF: counter-reset-corrected, extrapolated total increase.
#[must_use]
pub fn increase_udf() -> ScalarUDF {
    ScalarUDF::from(RateUdf::new(RateFamily::Increase))
}
