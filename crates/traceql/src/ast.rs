//! `TraceQL` abstract syntax tree.

// Intrinsic
#[derive(Clone, Debug, PartialEq, Eq)]
/// A field supplied by the trace data model rather than an attribute map.
pub enum Intrinsic {
    Name,
    Duration,
    Kind,
    Status,
    StatusMessage,
    Id,
    ParentId,
    ChildCount,
    TraceDuration,
    TraceRootName,
    TraceRootService,
    TraceId,
    EventName,
    EventTimeSinceStart,
    LinkTraceId,
    LinkSpanId,
    InstrumentationName,
    InstrumentationVersion,
    NestedSetLeft,
    NestedSetRight,
    NestedSetParent,
}

impl Intrinsic {
    /// Tempo's canonical tag name for this intrinsic, such as `span:parentID`.
    ///
    /// The tag-name API reports these names, and planner matchers key on them.
    #[must_use]
    pub fn tag_name(&self) -> &'static str {
        match self {
            Self::Name => "span:name",
            Self::Duration => "span:duration",
            Self::Kind => "span:kind",
            Self::Status => "span:status",
            Self::StatusMessage => "span:statusMessage",
            Self::Id => "span:id",
            Self::ParentId => "span:parentID",
            Self::ChildCount => "span:childCount",
            Self::TraceDuration => "trace:duration",
            Self::TraceRootName => "trace:rootName",
            Self::TraceRootService => "trace:rootService",
            Self::TraceId => "trace:id",
            Self::EventName => "event:name",
            Self::EventTimeSinceStart => "event:timeSinceStart",
            Self::LinkTraceId => "link:traceID",
            Self::LinkSpanId => "link:spanID",
            Self::InstrumentationName => "instrumentation:name",
            Self::InstrumentationVersion => "instrumentation:version",
            Self::NestedSetLeft => "span:nestedSetLeft",
            Self::NestedSetRight => "span:nestedSetRight",
            Self::NestedSetParent => "span:nestedSetParent",
        }
    }
}

#[cfg(test)]
mod query_tests {
    use assert2::check;

    use super::*;

    struct IntrinsicTagCase {
        intrinsic: Intrinsic,
        expected_name: &'static str,
    }

    /// The intrinsic tag names are Tempo's API surface: a client asking for
    /// `span:parentID` gets nothing back if this map spells it `span:parentId`.
    /// Every variant is named here, and the names are checked for being
    /// distinct as well as correct -- returning a neighbour's string is the
    /// failure a per-variant spot check misses. A new variant needs a row.
    #[test]
    fn every_intrinsic_maps_to_its_tempo_tag_name() {
        let cases = [
            IntrinsicTagCase {
                intrinsic: Intrinsic::Name,
                expected_name: "span:name",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::Duration,
                expected_name: "span:duration",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::Kind,
                expected_name: "span:kind",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::Status,
                expected_name: "span:status",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::StatusMessage,
                expected_name: "span:statusMessage",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::Id,
                expected_name: "span:id",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::ParentId,
                expected_name: "span:parentID",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::ChildCount,
                expected_name: "span:childCount",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::TraceDuration,
                expected_name: "trace:duration",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::TraceRootName,
                expected_name: "trace:rootName",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::TraceRootService,
                expected_name: "trace:rootService",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::TraceId,
                expected_name: "trace:id",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::EventName,
                expected_name: "event:name",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::EventTimeSinceStart,
                expected_name: "event:timeSinceStart",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::LinkTraceId,
                expected_name: "link:traceID",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::LinkSpanId,
                expected_name: "link:spanID",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::InstrumentationName,
                expected_name: "instrumentation:name",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::InstrumentationVersion,
                expected_name: "instrumentation:version",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::NestedSetLeft,
                expected_name: "span:nestedSetLeft",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::NestedSetRight,
                expected_name: "span:nestedSetRight",
            },
            IntrinsicTagCase {
                intrinsic: Intrinsic::NestedSetParent,
                expected_name: "span:nestedSetParent",
            },
        ];

        for IntrinsicTagCase {
            intrinsic,
            expected_name,
        } in &cases
        {
            check!(intrinsic.tag_name() == *expected_name, "{intrinsic:?}");
        }

        let names = cases
            .iter()
            .map(|case| case.expected_name)
            .collect::<std::collections::BTreeSet<_>>();
        check!(
            names.len() == cases.len(),
            "every intrinsic has its own name"
        );
    }
}

// Scope
#[derive(Clone, Debug, PartialEq, Eq)]
/// The `TraceQL` scope used to resolve a field.
pub enum Scope {
    Both,
    Span,
    Resource,
    Parent,
    Event,
    Link,
    Instrumentation,
    Intrinsic(Intrinsic),
}

// Field
#[derive(Clone, Debug, PartialEq, Eq)]
/// A scoped trace attribute reference.
pub struct Field {
    /// The resource, span, event, link, instrumentation, or intrinsic scope.
    pub scope: Scope,
    /// The attribute key inside the scope.
    pub key: String,
}

// Value
#[derive(Clone, Debug, PartialEq)]
/// A scalar literal in a `TraceQL` field expression.
pub enum Value {
    Str(String),
    Int(i64),
    Float(f64),
    Duration(i64),
    Bool(bool),
    Nil,
    /// Runtime array attribute; array literals are not part of the parser syntax.
    Array(Vec<Value>),
}

// Comparison Op
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// A `TraceQL` scalar or field comparison operator.
pub enum ComparisonOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Re,
    Nre,
}

/// A numeric comparison against a constant threshold.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct NumericFilter {
    pub(crate) op: ComparisonOp,
    pub(crate) value: f64,
}

// Scalar Expr
/// Arithmetic operation evaluated on the original typed span values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
}

/// A scalar expression; field references retain their scope and runtime type.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarExpr {
    Field(Field),
    Literal(Value),
    Predicate(Box<FieldExpr>),
    Negate(Box<ScalarExpr>),
    Binary {
        lhs: Box<ScalarExpr>,
        op: ArithmeticOp,
        rhs: Box<ScalarExpr>,
    },
}

impl ScalarExpr {
    pub(crate) fn collect_fields<'a>(&'a self, out: &mut Vec<&'a Field>) {
        match self {
            Self::Field(field) => out.push(field),
            Self::Predicate(expr) => expr.collect_fields(out),
            Self::Binary { lhs, rhs, .. } => {
                lhs.collect_fields(out);
                rhs.collect_fields(out);
            }
            Self::Negate(inner) => inner.collect_fields(out),
            Self::Literal(_) => {}
        }
    }
}

// Field Expr
#[derive(Clone, Debug, PartialEq)]
/// A Boolean expression over trace fields and scalar values.
pub enum FieldExpr {
    Comparison {
        lhs: Field,
        op: ComparisonOp,
        rhs: Value,
    },
    /// A comparison whose operands are resolved independently on each span.
    FieldComparison {
        lhs: Field,
        op: ComparisonOp,
        rhs: Field,
    },
    /// Scalar arithmetic comparison evaluated without SQL value coercion.
    ExpressionComparison {
        lhs: ScalarExpr,
        op: ComparisonOp,
        rhs: ScalarExpr,
    },
    And(Box<FieldExpr>, Box<FieldExpr>),
    Or(Box<FieldExpr>, Box<FieldExpr>),
    Not(Box<FieldExpr>),
    Field(Field),
    /// A constant boolean filter.
    ///
    /// The empty spanset `{}` and the scalar-boolean spanset `{ true }` lower
    /// to `Const(true)`, which matches every span. The spanset `{ false }`
    /// lowers to `Const(false)`, which matches no span. This mirrors Grafana
    /// Tempo, whose Explore "Search" tab and TraceQL-metrics default to `{}`.
    Const(bool),
}

impl FieldExpr {
    pub(crate) fn collect_fields<'a>(&'a self, out: &mut Vec<&'a Field>) {
        match self {
            Self::Comparison { lhs, .. } | Self::Field(lhs) => out.push(lhs),
            Self::FieldComparison { lhs, rhs, .. } => {
                out.push(lhs);
                out.push(rhs);
            }
            Self::ExpressionComparison { lhs, rhs, .. } => {
                lhs.collect_fields(out);
                rhs.collect_fields(out);
            }
            Self::And(lhs, rhs) | Self::Or(lhs, rhs) => {
                lhs.collect_fields(out);
                rhs.collect_fields(out);
            }
            Self::Not(inner) => inner.collect_fields(out),
            Self::Const(_) => {}
        }
    }

    pub(crate) fn has_field_comparison(&self) -> bool {
        match self {
            Self::FieldComparison { .. } | Self::ExpressionComparison { .. } => true,
            Self::And(lhs, rhs) | Self::Or(lhs, rhs) => {
                lhs.has_field_comparison() || rhs.has_field_comparison()
            }
            Self::Not(inner) => inner.has_field_comparison(),
            // Bare attributes are typed boolean predicates, rather than presence checks.
            Self::Field(field) => !matches!(field.scope, Scope::Parent),
            Self::Comparison { .. } | Self::Const(_) => false,
        }
    }
}

// Aggregate
#[derive(Clone, Debug, PartialEq)]
/// A `TraceQL` spanset or metrics aggregation.
pub enum Aggregate {
    Count,
    Rate,
    CountOverTime,
    SumOverTime(Field),
    AvgOverTime(Field),
    MinOverTime(Field),
    MaxOverTime(Field),
    HistogramOverTime(Field),
    QuantileOverTime {
        field: Field,
        quantiles: Vec<f64>,
    },
    /// A numeric expression evaluated on each original typed span.
    Expression {
        function: ScalarAggregate,
        expr: ScalarExpr,
    },
    Sum(Field),
    Avg(Field),
    Max(Field),
    Min(Field),
}

/// A scalar reduction over the spans in each current spanset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScalarAggregate {
    Sum,
    Avg,
    Min,
    Max,
}

impl Aggregate {
    pub(crate) fn is_metric(&self) -> bool {
        matches!(
            self,
            Self::Rate
                | Self::CountOverTime
                | Self::SumOverTime(_)
                | Self::AvgOverTime(_)
                | Self::MinOverTime(_)
                | Self::MaxOverTime(_)
                | Self::HistogramOverTime(_)
                | Self::QuantileOverTime { .. }
        )
    }
}

// With Binding
#[derive(Clone, Debug, PartialEq)]
/// A named field expression introduced by a `TraceQL` `with` pipeline.
pub struct WithBinding {
    /// The binding name.
    pub name: String,
    /// The expression assigned to the name.
    pub expr: FieldExpr,
}

// Pipeline
#[derive(Clone, Debug, PartialEq)]
/// A `TraceQL` pipeline operation applied after spanset selection.
pub enum Pipeline {
    Aggregate(Aggregate),
    Filter {
        op: ComparisonOp,
        value: f64,
    },
    By(Vec<Field>),
    /// Divide each current spanset by the original typed expression result.
    Group(ScalarExpr),
    TopK(usize),
    BottomK(usize),
    /// Tempo attribute-comparison metric: `compare({selection}, topN [, start_ns,
    /// end_ns])`.
    ///
    /// This metric splits the spans that match the outer spanset into two
    /// groups. The `selection` group holds the spans that also match
    /// `selection`. The `baseline` group holds the rest. The metric then emits
    /// per-attribute value-distribution series for each group. `top_n` keeps
    /// the most frequent values per attribute, and defaults to 10. The optional
    /// `start` and `end` nanosecond bounds narrow the selection sub-window.
    Compare {
        selection: Box<SpansetExpr>,
        top_n: usize,
        start: Option<i64>,
        end: Option<i64>,
    },
    Select(Vec<Field>),
    Coalesce,
    With(Vec<WithBinding>),
}

// Spanset Expr
#[derive(Clone, Debug, PartialEq)]
/// A `TraceQL` expression that selects or relates spansets.
pub enum SpansetExpr {
    Selector(Box<FieldExpr>),
    And(Box<SpansetExpr>, Box<SpansetExpr>),
    Or(Box<SpansetExpr>, Box<SpansetExpr>),
    Structural {
        op: StructuralOp,
        lhs: Box<SpansetExpr>,
        rhs: Box<SpansetExpr>,
    },
}

// Query Hints
#[derive(Clone, Debug, Default, PartialEq)]
/// Execution hints carried by a `TraceQL` query.
pub struct QueryHints {
    /// Requests the most recent matching spans first.
    pub most_recent: bool,
    /// Requests exemplar production when set.
    pub exemplars: Option<bool>,
    /// `with(sample=...)`: Tempo's probabilistic metrics-sampling hint.
    ///
    /// Grafana's Traces Drilldown sends `sample=true`. The parser accepts the
    /// boolean adaptive hint and computes exact metrics for it. Numeric fixed
    /// sampling fractions are stored in `values` and use the periodic estimator.
    pub sample: Option<bool>,
    /// Static hints not represented by the boolean execution switches.
    pub values: Vec<QueryHintValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryHintValue {
    pub name: String,
    pub value: Value,
}

impl QueryHints {
    pub(crate) fn numeric(&self, name: &str) -> Option<f64> {
        // GetFloat in pinned Tempo prefers the first float, then the first int.
        self.values
            .iter()
            .find_map(|hint| match &hint.value {
                Value::Float(value) if hint.name == name => Some(*value),
                _ => None,
            })
            .or_else(|| {
                self.values.iter().find_map(|hint| match &hint.value {
                    Value::Int(value) if hint.name == name => {
                        Some(value.to_string().parse().expect("signed integer is finite"))
                    }
                    _ => None,
                })
            })
    }
}

// Query
#[derive(Clone, Debug, PartialEq)]
/// A parsed `TraceQL` spanset expression, pipeline, and hint set.
pub struct Query {
    /// The spanset selection expression.
    pub root: SpansetExpr,
    /// The ordered pipeline operations.
    pub pipeline: Vec<Pipeline>,
    /// Optional execution hints from the query.
    pub hints: QueryHints,
}

// Structural Op
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// A structural relationship between two trace spansets.
pub enum StructuralOp {
    Descendant,
    Ancestor,
    Child,
    Parent,
    Sibling,
    NegDescendant,
    NegAncestor,
    NegChild,
    NegParent,
    NegSibling,
    UnionDescendant,
    UnionAncestor,
    UnionChild,
    UnionParent,
    UnionSibling,
}

// The full expression is the key: do not hash and risk aliasing two predicates.
pub(crate) fn field_comparison_column(expr: &FieldExpr) -> String {
    format!("__traceql_field_comparison_{expr:?}")
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn ast_constructs_selector_and_pipeline() {
        let q = Query {
            root: SpansetExpr::Selector(Box::new(FieldExpr::Comparison {
                lhs: Field {
                    scope: Scope::Both,
                    key: "foo".into(),
                },
                op: ComparisonOp::Eq,
                rhs: Value::Int(1),
            })),
            pipeline: vec![Pipeline::Aggregate(Aggregate::Count)],
            hints: QueryHints::default(),
        };
        assert!(matches!(q.root, SpansetExpr::Selector(_)));
        assert!(q.pipeline == vec![Pipeline::Aggregate(Aggregate::Count)]);
    }
}
