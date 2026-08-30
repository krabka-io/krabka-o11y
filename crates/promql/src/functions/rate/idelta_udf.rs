use super::{RateFamily, RateUdf, ScalarUDF};

/// The `idelta` UDF: gauge delta of the last two samples.
#[must_use]
pub fn idelta_udf() -> ScalarUDF {
    ScalarUDF::from(RateUdf::new(RateFamily::Idelta))
}
