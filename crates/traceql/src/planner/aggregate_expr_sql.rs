use super::*;

pub(crate) fn aggregate_expr_sql(agg: &Aggregate) -> Result<String> {
    let (func, field) = match agg {
        Aggregate::Sum(field) => ("SUM", field),
        Aggregate::Avg(field) => ("AVG", field),
        Aggregate::Min(field) => ("MIN", field),
        Aggregate::Max(field) => ("MAX", field),
        _ => {
            return Err(TraceqlError::Unsupported(format!(
                "aggregate {agg:?} is not supported in scalar filters"
            )));
        }
    };
    Ok(format!(
        "{func}({})",
        selector::ident(&selector::field_to_column(field))
    ))
}
