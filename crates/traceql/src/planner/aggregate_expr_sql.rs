use super::{Aggregate, Result, TraceqlError, selector};

pub(crate) fn aggregate_expr_sql(agg: &Aggregate) -> Result<String> {
    let field = match agg {
        Aggregate::Sum(field)
        | Aggregate::Avg(field)
        | Aggregate::Min(field)
        | Aggregate::Max(field) => field,
        _ => {
            return Err(TraceqlError::Unsupported(format!(
                "aggregate {agg:?} is not supported in scalar filters"
            )));
        }
    };
    let func = match agg {
        Aggregate::Sum(_) => "SUM",
        Aggregate::Avg(_) => "AVG",
        Aggregate::Min(_) => "MIN",
        Aggregate::Max(_) => "MAX",
        _ => unreachable!("validated above"),
    };
    Ok(format!(
        "{func}({})",
        selector::ident(&selector::field_to_column(field))
    ))
}
