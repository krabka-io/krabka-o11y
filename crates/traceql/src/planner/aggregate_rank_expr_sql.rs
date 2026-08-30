use super::*;

pub(crate) fn aggregate_rank_expr_sql(agg: &Aggregate) -> Result<String> {
    match agg {
        Aggregate::Count => Ok("COUNT(*)".to_string()),
        Aggregate::Sum(_) | Aggregate::Avg(_) | Aggregate::Min(_) | Aggregate::Max(_) => {
            aggregate_expr_sql(agg)
        }
        _ => Err(TraceqlError::Unsupported(format!(
            "aggregate {agg:?} is not supported in search ranking"
        ))),
    }
}
