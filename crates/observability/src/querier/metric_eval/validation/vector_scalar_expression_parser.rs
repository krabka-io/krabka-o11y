pub(crate) struct VectorScalarExpressionParser<'a> {
    pub(crate) input: &'a str,
    pub(crate) position: usize,
    pub(crate) vector_terms: usize,
}
