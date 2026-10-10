#[derive(Clone, Copy)]
pub(crate) enum QueryKind {
    Instant,
    Range,
}

impl QueryKind {
    /// The route label that query metrics record this kind of query under.
    pub(crate) fn route_label(self) -> &'static str {
        match self {
            Self::Instant => "query",
            Self::Range => "query_range",
        }
    }
}
