use super::*;

pub(crate) fn split_leading_vector_binary_modifiers(
    query: &str,
) -> (Option<FormattedVectorBinaryModifiers>, &str) {
    let Some((matching_modifier, rest)) = split_leading_vector_matching_modifier(query) else {
        return (None, query.trim_start());
    };
    let (group_modifier, rest) = split_leading_vector_group_modifier(rest);
    (
        Some(match group_modifier {
            Some(group_modifier) => FormattedVectorBinaryModifiers {
                text: format!("{matching_modifier} {group_modifier}"),
                right_separator: " ",
            },
            None => FormattedVectorBinaryModifiers {
                text: matching_modifier,
                right_separator: "  ",
            },
        }),
        rest.trim_start(),
    )
}
