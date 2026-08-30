use super::{RateFamily, RateUdf, ScalarUDF};

/// The `delta` UDF: gauge first..last delta with boundary extrapolation.
#[must_use]
pub fn delta_udf() -> ScalarUDF {
    ScalarUDF::from(RateUdf::new(RateFamily::Delta))
}
