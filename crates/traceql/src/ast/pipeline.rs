use super::*;

#[derive(Clone, Debug, PartialEq)]
pub enum Pipeline {
    Aggregate(Aggregate),
    Filter {
        op: ComparisonOp,
        value: f64,
    },
    By(Vec<Field>),
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
