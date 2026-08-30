use super::{ScalarUDF, RateUdf, RateFamily};

/// The `idelta` UDF: gauge delta of the last two samples.
#[must_use]
pub fn idelta_udf() -> ScalarUDF {
    ScalarUDF::from(RateUdf::new(RateFamily::Idelta))
}
