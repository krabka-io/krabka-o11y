use super::*;

pub(crate) fn unannotated(result: QueryResult) -> AnnotatedQueryResult {
    AnnotatedQueryResult {
        result,
        annotations: Annotations::new(),
    }
}
