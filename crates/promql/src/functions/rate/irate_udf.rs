use super::{RateFamily, RateUdf, ScalarUDF};

/// The `irate` UDF: per-second instant rate from the last two samples.
#[must_use]
pub fn irate_udf() -> ScalarUDF {
    ScalarUDF::from(RateUdf::new(RateFamily::Irate))
}
