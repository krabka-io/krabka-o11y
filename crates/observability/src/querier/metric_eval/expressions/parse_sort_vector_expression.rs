use super::{SortVectorExpression, split_logql_function_arguments};

pub(crate) fn parse_sort_vector_expression(query: &str) -> Option<SortVectorExpression> {
    for function_name in ["sort", "sort_desc"] {
        let descending = function_name == "sort_desc";
        let Some(arguments) = split_logql_function_arguments(query, function_name) else {
            continue;
        };
        let [inner_query] = arguments.as_slice() else {
            return None;
        };
        return Some(SortVectorExpression {
            query: inner_query.to_string(),
            descending,
        });
    }

    None
}
