use super::*;

pub(crate) fn enum_value_sql(scope: &Scope, name: &str) -> Result<String> {
    let normalized = name.to_ascii_lowercase();
    let value = match scope {
        Scope::Intrinsic(Intrinsic::Status) => status_enum_value(&normalized),
        Scope::Intrinsic(Intrinsic::Kind) => kind_enum_value(&normalized),
        _ => {
            return Err(TraceqlError::Plan(format!(
                "unknown {} enum value {name:?}",
                intrinsic_name(scope)
            )));
        }
    };
    value.map(|v| v.to_string()).ok_or_else(|| {
        TraceqlError::Plan(format!(
            "unknown {} enum value {name:?}",
            intrinsic_name(scope)
        ))
    })
}
