use super::{
    FormattedVectorBinaryModifiers, SplitLeadingVectorBinaryModifiers,
    split_leading_vector_group_modifier, split_leading_vector_matching_modifier,
};

pub(crate) fn split_leading_vector_binary_modifiers(
    query: &str,
) -> SplitLeadingVectorBinaryModifiers<'_> {
    let Some(matching_modifier) = split_leading_vector_matching_modifier(query) else {
        return SplitLeadingVectorBinaryModifiers {
            modifiers: None,
            remainder: query.trim_start(),
        };
    };
    let group_modifier = split_leading_vector_group_modifier(matching_modifier.remainder);
    SplitLeadingVectorBinaryModifiers {
        modifiers: Some(match group_modifier.text {
            Some(group_modifier) => FormattedVectorBinaryModifiers {
                text: format!("{} {group_modifier}", matching_modifier.text),
                right_separator: " ",
            },
            None => FormattedVectorBinaryModifiers {
                text: matching_modifier.text,
                right_separator: "  ",
            },
        }),
        remainder: group_modifier.remainder.trim_start(),
    }
}
