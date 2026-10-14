pub(crate) mod aggregation_formatting;
pub(crate) mod metric_formatting;
mod quoted_text_scanner;
pub(crate) mod request_formatting;
pub(crate) mod stream_formatting;

pub(crate) use quoted_text_scanner::QuotedTextScanner;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ParsedValueAt<T> {
    pub(crate) value: T,
    pub(crate) position: usize,
}

pub(crate) struct VectorBinaryQueryParts<'a> {
    pub(crate) left: &'a str,
    pub(crate) operator: &'a str,
    pub(crate) modifiers: Option<aggregation_formatting::FormattedVectorBinaryModifiers>,
    pub(crate) right: &'a str,
}

pub(crate) struct VectorComparisonQueryParts<'a> {
    pub(crate) left: &'a str,
    pub(crate) operator: &'a str,
    pub(crate) bool_modifier: bool,
    pub(crate) modifiers: Option<aggregation_formatting::FormattedVectorBinaryModifiers>,
    pub(crate) right: &'a str,
}

pub(crate) fn split_vector_comparison_query(query: &str) -> Option<VectorComparisonQueryParts<'_>> {
    let split = metric_formatting::split_top_level_comparison_query(query)?;
    let right = split.right.trim_start();
    let bool_modifier = right.starts_with("bool");
    let right = if bool_modifier {
        right.strip_prefix("bool")?.trim_start()
    } else {
        right
    };
    let modifiers = request_formatting::split_leading_vector_binary_modifiers(right);
    Some(VectorComparisonQueryParts {
        left: split.left,
        operator: split.operator,
        bool_modifier,
        modifiers: modifiers.modifiers,
        right: modifiers.remainder,
    })
}

pub(crate) fn split_vector_binary_query<'a>(
    query: &'a str,
    split: impl FnOnce(&'a str) -> Option<metric_formatting::TopLevelQuerySplit<'a>>,
) -> Option<VectorBinaryQueryParts<'a>> {
    let split = split(query)?;
    let modifiers = request_formatting::split_leading_vector_binary_modifiers(split.right);
    Some(VectorBinaryQueryParts {
        left: split.left,
        operator: split.operator,
        modifiers: modifiers.modifiers,
        right: modifiers.remainder,
    })
}
