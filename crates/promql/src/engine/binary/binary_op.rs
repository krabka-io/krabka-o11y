use super::{
    BinModifier, InstantSample, Labels, NativeHistogram, PromqlError, Result, SampleValue,
    ScalarSide, T_ADD, T_ATAN2, T_DIV, T_EQLC, T_GTE, T_GTR, T_LAND, T_LOR, T_LSS, T_LTE,
    T_LUNLESS, T_MOD, T_MUL, T_NEQ, T_POW, T_SUB, TokenType, binary_returns_bool, emit_info,
    float_sample_value, incompatible_types_in_binop_info, labels_without_metric_name,
    scaled_native_histogram,
};

#[derive(Clone, Copy)]
pub(crate) enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Atan2,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
}

impl BinaryOp {
    pub(crate) fn try_from_token(token: TokenType) -> Result<Self> {
        match token.id() {
            T_ADD => Ok(Self::Add),
            T_SUB => Ok(Self::Sub),
            T_MUL => Ok(Self::Mul),
            T_DIV => Ok(Self::Div),
            T_MOD => Ok(Self::Mod),
            T_POW => Ok(Self::Pow),
            T_ATAN2 => Ok(Self::Atan2),
            T_EQLC => Ok(Self::Eq),
            T_NEQ => Ok(Self::Neq),
            T_GTR => Ok(Self::Gt),
            T_LSS => Ok(Self::Lt),
            T_GTE => Ok(Self::Gte),
            T_LTE => Ok(Self::Lte),
            // Unreachable by construction: `combine_instant_binary` routes every
            // set operator through `SetOp::from_token` before it gets here, so
            // no query reaches this arm and a sweep will always report it as a
            // survivor. It stays as the diagnostic for that routing breaking.
            T_LAND | T_LOR | T_LUNLESS => Err(PromqlError::Plan(format!(
                "set operator `{token}` reached the arithmetic operator path"
            ))),
            _ => Err(PromqlError::Unsupported(format!(
                "unsupported binary operator `{token}`"
            ))),
        }
    }

    pub(crate) fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::Neq | Self::Gt | Self::Lt | Self::Gte | Self::Lte
        )
    }

    /// Returns the `PromQL` surface symbol for this operator.
    ///
    /// The symbol matches the Prometheus annotation text, for example `==`,
    /// `!=`, `>`, and `>=`.
    pub(crate) fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Pow => "^",
            Self::Atan2 => "atan2",
            Self::Eq => "==",
            Self::Neq => "!=",
            Self::Gt => ">",
            Self::Lt => "<",
            Self::Gte => ">=",
            Self::Lte => "<=",
        }
    }

    pub(crate) fn apply_scalar(
        self,
        left: f64,
        right: f64,
        modifier: Option<&BinModifier>,
    ) -> Option<f64> {
        if self.is_comparison() {
            let pass = self.compare(left, right);
            if binary_returns_bool(modifier) {
                Some(if pass { 1.0 } else { 0.0 })
            } else if pass {
                Some(left)
            } else {
                None
            }
        } else {
            Some(self.arithmetic(left, right))
        }
    }

    pub(crate) fn apply_vector_scalar(
        self,
        sample: InstantSample,
        scalar: f64,
        modifier: Option<&BinModifier>,
        scalar_side: ScalarSide,
    ) -> Option<InstantSample> {
        if let SampleValue::Histogram(histogram) = sample.value {
            return self.apply_histogram_scalar(
                &sample.labels,
                sample.ts_ms,
                &histogram,
                scalar,
                scalar_side,
            );
        }

        let sample_value = float_sample_value(&sample).ok()?;
        let (left, right) = match scalar_side {
            ScalarSide::Left => (scalar, sample_value),
            ScalarSide::Right => (sample_value, scalar),
        };
        let value = if self.is_comparison() && !binary_returns_bool(modifier) {
            self.compare(left, right).then_some(sample_value)?
        } else {
            self.apply_scalar(left, right, modifier)?
        };
        let labels = if self.is_comparison() && !binary_returns_bool(modifier) {
            sample.labels
        } else {
            labels_without_metric_name(&sample.labels)
        };
        Some(InstantSample {
            labels,
            ts_ms: sample.ts_ms,
            value: SampleValue::Float(value),
        })
    }

    pub(crate) fn apply_histogram_scalar(
        self,
        labels: &Labels,
        ts_ms: i64,
        histogram: &NativeHistogram,
        scalar: f64,
        scalar_side: ScalarSide,
    ) -> Option<InstantSample> {
        let factor = match (self, scalar_side) {
            (Self::Mul, ScalarSide::Left | ScalarSide::Right) => scalar,
            (Self::Div, ScalarSide::Right) => 1.0 / scalar,
            _ => {
                if self.is_comparison() {
                    // Prometheus ignores the histogram operand in a comparison
                    // against a float, dropping the sample and raising an info.
                    let (lhs, rhs) = match scalar_side {
                        ScalarSide::Left => ("float", "histogram"),
                        ScalarSide::Right => ("histogram", "float"),
                    };
                    emit_info(incompatible_types_in_binop_info(lhs, self.symbol(), rhs));
                }
                return None;
            }
        };
        Some(InstantSample {
            labels: labels_without_metric_name(labels),
            ts_ms,
            value: SampleValue::Histogram(scaled_native_histogram(histogram, factor)),
        })
    }

    pub(crate) fn arithmetic(self, left: f64, right: f64) -> f64 {
        match self {
            Self::Add => left + right,
            Self::Sub => left - right,
            Self::Mul => left * right,
            Self::Div => left / right,
            Self::Mod => left % right,
            Self::Pow => left.powf(right),
            Self::Atan2 => left.atan2(right),
            Self::Eq | Self::Neq | Self::Gt | Self::Lt | Self::Gte | Self::Lte => {
                unreachable!("comparison op used as arithmetic")
            }
        }
    }

    pub(crate) fn compare(self, left: f64, right: f64) -> bool {
        match self {
            Self::Eq => left
                .partial_cmp(&right)
                .is_some_and(std::cmp::Ordering::is_eq),
            Self::Neq => !left
                .partial_cmp(&right)
                .is_some_and(std::cmp::Ordering::is_eq),
            Self::Gt => left > right,
            Self::Lt => left < right,
            Self::Gte => left >= right,
            Self::Lte => left <= right,
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Mod | Self::Pow | Self::Atan2 => {
                unreachable!("arithmetic op used as comparison")
            }
        }
    }
}
