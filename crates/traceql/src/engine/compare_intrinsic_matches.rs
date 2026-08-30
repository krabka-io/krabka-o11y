use super::*;

pub(crate) fn compare_intrinsic_matches(
    row: &CompareRow,
    intrinsic: &Intrinsic,
    op: ComparisonOp,
    rhs: &Value,
    regexes: &CompareRegexCache,
) -> bool {
    match intrinsic {
        Intrinsic::Name => row.name.as_ref().is_some_and(
            |name| matches!(rhs, Value::Str(rhs) if string_cmp(name, op, rhs, regexes)),
        ),
        Intrinsic::StatusMessage => row
            .status_message
            .as_ref()
            .is_some_and(|msg| matches!(rhs, Value::Str(rhs) if string_cmp(msg, op, rhs, regexes))),
        Intrinsic::Status => row
            .status_code
            .is_some_and(|code| enum_cmp(code, op, rhs, status_enum_value)),
        Intrinsic::Kind => row
            .kind
            .is_some_and(|code| enum_cmp(code, op, rhs, kind_enum_value)),
        Intrinsic::Duration => row.duration.is_some_and(|duration| match rhs {
            Value::Int(rhs) | Value::Duration(rhs) => num_cmp(duration, op, *rhs),
            _ => false,
        }),
        _ => false,
    }
}
