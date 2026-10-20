use super::{ComparisonOp, MetricScalarArithmeticOp, ScalarSide};

/// What a comparison operator answers with.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ComparisonResult {
    /// Without the `bool` modifier: the samples that match, unchanged, and no
    /// others.
    Filter,
    /// With the `bool` modifier: every sample, as `1` where it matches and
    /// `0` where it does not.
    Bool,
}

impl ComparisonResult {
    /// The result `krabka_logql`'s `bool_modifier` flag names.
    pub(crate) fn from_bool_modifier(bool_modifier: bool) -> Self {
        if bool_modifier {
            Self::Bool
        } else {
            Self::Filter
        }
    }
}

/// One comparison operator and what it answers with.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MetricComparison {
    pub(crate) op: ComparisonOp,
    pub(crate) result: ComparisonResult,
}

/// A comparison of every sample against a scalar literal.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ScalarComparison {
    pub(crate) comparison: MetricComparison,
    pub(crate) scalar_side: ScalarSide,
}

/// An arithmetic operator applied to every sample and a scalar literal.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ScalarArithmetic {
    pub(crate) op: MetricScalarArithmeticOp,
    pub(crate) scalar_side: ScalarSide,
}

#[cfg(test)]
impl From<&super::MetricScalarComparison> for ScalarComparison {
    fn from(comparison: &super::MetricScalarComparison) -> Self {
        Self {
            comparison: MetricComparison {
                op: comparison.op,
                result: ComparisonResult::from_bool_modifier(comparison.bool_modifier),
            },
            scalar_side: ScalarSide::from_scalar_on_left(comparison.scalar_on_left),
        }
    }
}
