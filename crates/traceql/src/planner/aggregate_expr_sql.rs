use super::{Aggregate, Result, TraceqlError, selector};

pub(crate) fn aggregate_expr_sql(agg: &Aggregate) -> Result<String> {
    let (field, func) = match agg {
        Aggregate::Sum(field) => (field, "SUM"),
        Aggregate::Avg(field) => (field, "AVG"),
        Aggregate::Min(field) => (field, "MIN"),
        Aggregate::Max(field) => (field, "MAX"),
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
