use super::AnnotationExpect;

/// One `expect` directive from the expectation block of an eval statement.
///
/// Prometheus promqltest writes annotation expectations and the result-order
/// expectation with the same `expect` keyword, but they check different things.
/// An annotation directive reads the annotations that the query raised. The
/// order directive changes how the harness compares the result itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ExpectDirective {
    /// An annotation directive: `warn`, `info`, `no_warn`, `no_info`, or a
    /// `msg:` form of `warn` or `info`.
    Annotation(AnnotationExpect),
    /// `expect ordered`: the result series must come back in the order that the
    /// expectation lines are written in.
    Ordered,
}
