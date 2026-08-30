use super::{LogicalPlan, VALUE_COLUMN, TIME_COLUMN, SAMPLE_TIME_COLUMN};

/// The `Utf8` label columns of an inner plan's output schema, in schema order.
///
/// The `value`/`timestamp`/`sample_timestamp` columns are the index and value
/// columns of the operator chain. They are never labels.
pub(crate) fn input_label_columns(input: &LogicalPlan) -> Vec<String> {
    input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| name != VALUE_COLUMN && name != TIME_COLUMN && name != SAMPLE_TIME_COLUMN)
        .collect()
}
