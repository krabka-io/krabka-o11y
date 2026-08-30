use super::*;

/// A recursively composable `LogQL` expression.
#[derive(Clone, Debug, PartialEq)]
pub enum LogqlExpr {
    Stream {
        query: StreamQuery,
        source: String,
    },
    Metric {
        query: MetricQuery,
        source: String,
    },
    Scalar(String),
    Vector(Box<LogqlExpr>),
    LabelReplace {
        expr: Box<LogqlExpr>,
        destination_label: String,
        replacement: String,
        source_label: String,
        pattern: String,
    },
    LabelJoin {
        expr: Box<LogqlExpr>,
        destination_label: String,
        separator: String,
        source_labels: Vec<String>,
    },
    Sort {
        expr: Box<LogqlExpr>,
        descending: bool,
    },
    Arithmetic {
        left: Box<LogqlExpr>,
        op: MetricScalarArithmeticOp,
        matching: Option<MetricVectorMatching>,
        right: Box<LogqlExpr>,
    },
    Comparison {
        left: Box<LogqlExpr>,
        op: ComparisonOp,
        bool_modifier: bool,
        matching: Option<MetricVectorMatching>,
        right: Box<LogqlExpr>,
    },
    Set {
        left: Box<LogqlExpr>,
        op: MetricBinarySetOp,
        matching: Option<MetricVectorMatching>,
        right: Box<LogqlExpr>,
    },
}

impl LogqlExpr {
    /// Return the original source for a stream or metric leaf.
    #[must_use]
    pub fn source(&self) -> Option<&str> {
        match self {
            Self::Stream { source, .. } | Self::Metric { source, .. } => Some(source),
            _ => None,
        }
    }
    pub(crate) fn is_scalar(&self) -> bool {
        match self {
            Self::Scalar(_) => true,
            Self::Arithmetic {
                left,
                matching: None,
                right,
                ..
            }
            | Self::Comparison {
                left,
                bool_modifier: true,
                matching: None,
                right,
                ..
            } => left.is_scalar() && right.is_scalar(),
            _ => false,
        }
    }
    pub(crate) fn precedence(&self) -> u8 {
        match self {
            Self::Set {
                op: MetricBinarySetOp::Or,
                ..
            } => 1,
            Self::Set { .. } => 2,
            Self::Comparison { .. } => 3,
            Self::Arithmetic { op, .. } => match op {
                MetricScalarArithmeticOp::Add | MetricScalarArithmeticOp::Subtract => 4,
                MetricScalarArithmeticOp::Multiply
                | MetricScalarArithmeticOp::Divide
                | MetricScalarArithmeticOp::Modulo => 5,
                MetricScalarArithmeticOp::Power => 6,
            },
            _ => 7,
        }
    }
    pub(crate) fn format_at(&self, f: &mut fmt::Formatter<'_>, parent: u8, right: bool) -> fmt::Result {
        let precedence = self.precedence();
        let needs_parentheses = precedence < parent
            || (right
                && precedence == parent
                && !matches!(
                    self,
                    Self::Arithmetic {
                        op: MetricScalarArithmeticOp::Power,
                        ..
                    }
                ));
        if needs_parentheses {
            write!(f, "(")?;
        }
        match self {
            Self::Stream { source, .. } | Self::Metric { source, .. } | Self::Scalar(source) => {
                write!(f, "{}", source.trim())?;
            }
            Self::Vector(expr) => {
                write!(f, "vector(")?;
                expr.format_at(f, 0, false)?;
                write!(f, ")")?;
            }
            Self::Sort { expr, descending } => {
                write!(f, "{}(", if *descending { "sort_desc" } else { "sort" })?;
                expr.format_at(f, 0, false)?;
                write!(f, ")")?;
            }
            Self::LabelReplace {
                expr,
                destination_label,
                replacement,
                source_label,
                pattern,
            } => {
                write!(f, "label_replace(")?;
                expr.format_at(f, 0, false)?;
                write!(
                    f,
                    ", {}, {}, {}, {})",
                    Quoted(destination_label),
                    Quoted(replacement),
                    Quoted(source_label),
                    Quoted(pattern)
                )?;
            }
            Self::LabelJoin {
                expr,
                destination_label,
                separator,
                source_labels,
            } => {
                write!(f, "label_join(")?;
                expr.format_at(f, 0, false)?;
                write!(f, ", {}, {}", Quoted(destination_label), Quoted(separator))?;
                for label in source_labels {
                    write!(f, ", {}", Quoted(label))?;
                }
                write!(f, ")")?;
            }
            Self::Arithmetic {
                left,
                op,
                matching,
                right,
            } => {
                left.format_at(
                    f,
                    if matches!(op, MetricScalarArithmeticOp::Power) {
                        precedence.saturating_add(1)
                    } else {
                        precedence
                    },
                    false,
                )?;
                write!(f, " {}", arithmetic_text(*op))?;
                format_matching(f, matching.as_ref())?;
                write!(f, " ")?;
                right.format_at(
                    f,
                    precedence,
                    !matches!(op, MetricScalarArithmeticOp::Power),
                )?;
            }
            Self::Comparison {
                left,
                op,
                bool_modifier,
                matching,
                right,
            } => {
                left.format_at(f, precedence, false)?;
                write!(f, " {}", comparison_text(*op))?;
                if *bool_modifier {
                    write!(f, " bool")?;
                }
                format_matching(f, matching.as_ref())?;
                write!(f, " ")?;
                right.format_at(f, precedence, true)?;
            }
            Self::Set {
                left,
                op,
                matching,
                right,
            } => {
                left.format_at(f, precedence, false)?;
                write!(f, " {}", set_text(*op))?;
                format_matching(f, matching.as_ref())?;
                write!(f, " ")?;
                right.format_at(f, precedence, true)?;
            }
        }
        if needs_parentheses {
            write!(f, ")")?;
        }
        Ok(())
    }
}

impl fmt::Display for LogqlExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.format_at(f, 0, false)
    }
}
