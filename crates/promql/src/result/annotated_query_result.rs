use super::{Annotations, QueryResult};

/// A query result and the annotations that its evaluation raised.
///
/// The query frontend carries this pair through its executor, its cache, and its
/// merge step. A cached result then reports the same warnings and infos as the
/// evaluation that stored it.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct AnnotatedQueryResult {
    /// The result of the evaluation.
    pub result: QueryResult,
    /// The warnings and infos that the evaluation raised.
    pub annotations: Annotations,
}
