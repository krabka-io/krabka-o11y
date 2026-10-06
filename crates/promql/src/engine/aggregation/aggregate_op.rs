// Constants, so `try_from_token` matches on them rather than binding them.
use krabka_domain_macros::EnumName;
#[cfg(test)]
use promql_parser::parser::token::{
    T_AVG, T_COUNT, T_GROUP, T_MAX, T_MIN, T_STDDEV, T_STDVAR, T_SUM,
};

use super::{AggregateState, SampleValue};
#[cfg(test)]
use super::{PromqlError, Result, TokenType};

#[derive(Clone, Copy, EnumName)]
#[enum_name(accessor = "name")]
pub(crate) enum AggregateOp {
    #[name(value = "sum")]
    Sum,
    #[name(value = "avg")]
    Avg,
    #[name(value = "count")]
    Count,
    #[name(value = "group")]
    Group,
    #[name(value = "min")]
    Min,
    #[name(value = "max")]
    Max,
    #[name(value = "stddev")]
    Stddev,
    #[name(value = "stdvar")]
    Stdvar,
}

impl AggregateOp {
    #[cfg(test)]
    pub(crate) fn try_from_token(token: TokenType) -> Result<Self> {
        match token.id() {
            T_SUM => Ok(Self::Sum),
            T_AVG => Ok(Self::Avg),
            T_COUNT => Ok(Self::Count),
            T_GROUP => Ok(Self::Group),
            T_MIN => Ok(Self::Min),
            T_MAX => Ok(Self::Max),
            T_STDDEV => Ok(Self::Stddev),
            T_STDVAR => Ok(Self::Stdvar),
            _ => Err(PromqlError::Unsupported(format!(
                "unsupported simple aggregation `{token}`"
            ))),
        }
    }

    pub(crate) fn finish(self, state: &AggregateState) -> Option<SampleValue> {
        if state.count == 0
            || state.invalid_mixed_sample_type
            || state.invalid_mixed_histogram_schema
        {
            return None;
        }
        Some(match self {
            Self::Sum => match &state.histogram {
                Some(histogram) => SampleValue::Histogram(histogram.finish(false)),
                None => SampleValue::Float(state.sum + state.sum_comp),
            },
            Self::Avg => match &state.histogram {
                Some(histogram) => SampleValue::Histogram(histogram.finish(true)),
                None => SampleValue::Float(state.mean()),
            },
            Self::Count => SampleValue::Float(state.count_f64),
            Self::Group => SampleValue::Float(1.0),
            Self::Min => SampleValue::Float(state.min),
            Self::Max => SampleValue::Float(state.max),
            Self::Stddev => SampleValue::Float(state.population_variance().sqrt()),
            Self::Stdvar => SampleValue::Float(state.population_variance()),
        })
    }

    pub(crate) fn ignores_histograms(self) -> bool {
        matches!(self, Self::Min | Self::Max | Self::Stddev | Self::Stdvar)
    }

    pub(crate) fn counts_histograms(self) -> bool {
        matches!(self, Self::Count | Self::Group)
    }

    pub(crate) fn aggregates_histograms(self) -> bool {
        matches!(self, Self::Sum | Self::Avg)
    }
}
