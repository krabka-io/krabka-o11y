use super::prelude::{
    check, formatted_sample, instant_series, json, range_pairs, range_series, timed_sample,
};

fn split_matches(
    actual: Option<crate::http::params_format::metric_formatting::TopLevelQuerySplit<'_>>,
    left: &str,
    operator: &str,
    right: &str,
) -> bool {
    actual.is_some_and(|split| {
        split.left == left && split.operator == operator && split.right == right
    })
}

mod a_binary_set_operator_keeps_the_subset_it_names;
mod a_top_level_arithmetic_split_names_the_operator_it_found;
mod a_top_level_comparison_ignores_operators_nested_inside_the_query;
mod a_top_level_set_split_needs_a_whole_word;
mod a_vector_comparison_filters_and_takes_the_left_operand;
mod vector_arithmetic_computes_left_op_right_where_both_have_a_sample;
