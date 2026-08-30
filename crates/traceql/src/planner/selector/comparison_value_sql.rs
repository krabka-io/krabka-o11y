use super::*;

pub(crate) fn comparison_value_sql(field: &Field, value: &Value) -> Result<String> {
    if matches!(
        field.scope,
        Scope::Intrinsic(Intrinsic::Kind | Intrinsic::Status)
    ) && let Value::Str(name) = value
    {
        return enum_value_sql(&field.scope, name);
    }
    let width = match field.scope {
        Scope::Intrinsic(Intrinsic::TraceId | Intrinsic::LinkTraceId) => Some(16),
        Scope::Intrinsic(Intrinsic::Id | Intrinsic::ParentId | Intrinsic::LinkSpanId) => Some(8),
        _ => None,
    };
    if let Some(width) = width {
        let Value::Str(hex) = value else {
            return Err(TraceqlError::Plan(format!(
                "{} comparisons require a hex string value",
                intrinsic_name(&field.scope)
            )));
        };
        return fixed_hex_lit(hex, width);
    }
    value_sql(value)
}
