use super::*;

pub(crate) fn aggregate_filter_sql(expr: &str, op: ComparisonOp, value: f64) -> Result<String> {
    if !value.is_finite() {
        return Err(TraceqlError::Plan(
            "pipeline filter value is not finite".into(),
        ));
    }
    let op = match op {
        ComparisonOp::Eq => "=",
        ComparisonOp::Neq => "!=",
        ComparisonOp::Lt => "<",
        ComparisonOp::Lte => "<=",
        ComparisonOp::Gt => ">",
        ComparisonOp::Gte => ">=",
        ComparisonOp::Re | ComparisonOp::Nre => {
            return Err(TraceqlError::Unsupported(
                "regex filter on pipeline scalar is not supported".into(),
            ));
        }
    };
    Ok(format!("{expr} {op} {value}"))
}
