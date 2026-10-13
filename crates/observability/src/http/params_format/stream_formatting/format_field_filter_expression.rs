use super::{FieldFilterExpression, FieldFilterLogicOp, format_field_filter};

pub(crate) fn format_field_filter_expression(expression: &FieldFilterExpression) -> String {
    match expression {
        FieldFilterExpression::Filter(filter) => format_field_filter(filter),
        FieldFilterExpression::Group(expression) => {
            format!("({})", format_field_filter_expression(expression))
        }
        FieldFilterExpression::Chain { first, rest } => {
            let mut formatted = format_field_filter_expression(first);
            for step in rest {
                formatted.push_str(match step.operation {
                    FieldFilterLogicOp::And => " and ",
                    FieldFilterLogicOp::Or => " or ",
                });
                formatted.push_str(&format_field_filter_expression(&step.filter));
            }
            formatted
        }
    }
}
