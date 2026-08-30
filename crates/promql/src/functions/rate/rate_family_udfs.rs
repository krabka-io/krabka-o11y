use super::{ScalarUDF, rate_udf, increase_udf, delta_udf, irate_udf, idelta_udf};

/// Every rate-family UDF, ready to register on a [`SessionContext`].
#[must_use]
pub fn rate_family_udfs() -> Vec<ScalarUDF> {
    vec![
        rate_udf(),
        increase_udf(),
        delta_udf(),
        irate_udf(),
        idelta_udf(),
    ]
}
