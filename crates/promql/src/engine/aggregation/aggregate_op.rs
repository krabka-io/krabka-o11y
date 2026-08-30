use super::*;

#[derive(Clone, Copy)]
pub(crate) enum AggregateOp {
    Sum,
    Avg,
    Count,
    Group,
    Min,
    Max,
    Stddev,
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
        if state.count == 0 || state.invalid_mixed_sample_type {
            return None;
        }
        Some(match self {
            Self::Sum => match &state.histogram {
                Some(histogram) => SampleValue::Histogram(histogram.clone()),
                None => SampleValue::Float(state.sum),
            },
            Self::Avg => match &state.histogram {
                Some(histogram) => SampleValue::Histogram(scaled_native_histogram(
                    histogram,
                    1.0 / state.count_f64,
                )),
                None => SampleValue::Float(state.avg_mean + state.avg_comp),
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
