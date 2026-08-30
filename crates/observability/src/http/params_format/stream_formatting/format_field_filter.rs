use super::*;

pub(crate) fn format_field_filter(filter: &FieldFilter) -> String {
    format!(
        "{}{}{}",
        filter.name,
        match filter.op {
            ComparisonOp::Equal => "=",
            ComparisonOp::NotEqual => "!=",
            ComparisonOp::RegexEqual => "=~",
            ComparisonOp::RegexNotEqual => "!~",
            ComparisonOp::Greater => ">",
            ComparisonOp::GreaterEqual => ">=",
            ComparisonOp::Less => "<",
            ComparisonOp::LessEqual => "<=",
        },
        match &filter.value {
            FieldValue::Number(value) => value.to_string(),
            FieldValue::Duration(value) => format!("{value}ns"),
            FieldValue::Bytes(value) => format!("{}B", value.bytes_f64()),
            FieldValue::String(value) => quote_logql_string(value),
            FieldValue::Ip(value) => format!("ip({})", quote_logql_string(value.pattern())),
        }
    )
}
