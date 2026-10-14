use super::{
    FormattedVectorBinaryModifiers, parse_vector_group_modifier, parse_vector_matching_modifier,
};
use crate::http::params_format::ParsedValueAt;

pub(crate) fn parse_vector_binary_modifiers(
    query: &str,
    position: usize,
) -> Option<ParsedValueAt<FormattedVectorBinaryModifiers>> {
    let matching_modifier = parse_vector_matching_modifier(query, position)?;
    if let Some(group_modifier) = parse_vector_group_modifier(query, matching_modifier.position) {
        return Some(ParsedValueAt {
            value: FormattedVectorBinaryModifiers {
                text: format!("{} {}", matching_modifier.value, group_modifier.value),
                right_separator: " ",
            },
            position: group_modifier.position,
        });
    }
    Some(ParsedValueAt {
        value: FormattedVectorBinaryModifiers {
            text: matching_modifier.value,
            right_separator: "  ",
        },
        position: matching_modifier.position,
    })
}
