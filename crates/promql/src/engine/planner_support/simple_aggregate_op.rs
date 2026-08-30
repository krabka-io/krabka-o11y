use super::{SimpleAggregateOp, T_AVG, T_COUNT, T_GROUP, T_MAX, T_MIN, T_SUM, TokenType};

/// Maps an aggregation token to its simple-aggregation lowering. Returns `None`
/// for an op outside the simple set, such as a param op, `stddev`, or `stdvar`.
pub(crate) fn simple_aggregate_op(token: TokenType) -> Option<SimpleAggregateOp> {
    match token.id() {
        T_SUM => Some(SimpleAggregateOp::Sum),
        T_AVG => Some(SimpleAggregateOp::Avg),
        T_MIN => Some(SimpleAggregateOp::Min),
        T_MAX => Some(SimpleAggregateOp::Max),
        T_COUNT => Some(SimpleAggregateOp::Count),
        T_GROUP => Some(SimpleAggregateOp::Group),
        _ => None,
    }
}
