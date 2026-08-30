use super::*;

/// Which rate-family `ScalarUDF` a range-selector plan projects.
///
/// The registered UDF names (`prom_rate`, …) are the seam to
/// [`crate::functions::rate`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RateUdfKind {
    Rate,
    Increase,
    Delta,
    Irate,
    Idelta,
}

impl RateUdfKind {
    /// The registered UDF name this kind projects.
    pub(crate) fn udf_name(self) -> &'static str {
        match self {
            Self::Rate => "prom_rate",
            Self::Increase => "prom_increase",
            Self::Delta => "prom_delta",
            Self::Irate => "prom_irate",
            Self::Idelta => "prom_idelta",
        }
    }

    /// Resolves the matrix-selector `PromQL` function name to its UDF kind.
    ///
    /// This function returns `None` for any function outside the operator-path
    /// rate family.
    #[must_use]
    pub fn from_function_name(name: &str) -> Option<Self> {
        match name {
            "rate" => Some(Self::Rate),
            "increase" => Some(Self::Increase),
            "delta" => Some(Self::Delta),
            "irate" => Some(Self::Irate),
            "idelta" => Some(Self::Idelta),
            _ => None,
        }
    }
}
