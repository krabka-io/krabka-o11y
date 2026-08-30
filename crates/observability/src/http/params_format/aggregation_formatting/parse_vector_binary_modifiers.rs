use super::*;

pub(crate) fn parse_vector_binary_modifiers(
    query: &str,
    position: usize,
) -> Option<(FormattedVectorBinaryModifiers, usize)> {
    let (matching_modifier, position) = parse_vector_matching_modifier(query, position)?;
    if let Some((group_modifier, position)) = parse_vector_group_modifier(query, position) {
        return Some((
            FormattedVectorBinaryModifiers {
                text: format!("{matching_modifier} {group_modifier}"),
                right_separator: " ",
            },
            position,
        ));
    }
    Some((
        FormattedVectorBinaryModifiers {
            text: matching_modifier,
            right_separator: "  ",
        },
        position,
    ))
}
