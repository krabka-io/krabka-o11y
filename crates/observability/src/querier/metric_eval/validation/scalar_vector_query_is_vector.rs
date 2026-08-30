use super::*;

pub(crate) fn scalar_vector_query_is_vector(query: &str) -> bool {
    matches!(
        scalar_vector_expression_result(query),
        Some(ScalarVectorExpressionResult::Vector { .. })
    )
}
