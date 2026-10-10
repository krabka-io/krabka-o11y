use super::{
    BinaryExpr, BinaryOp, InstantOperands, InstantValue, PromqlError, QueryResult, Result,
    ScalarSide, SetOp, VectorMatching, VectorOperands, eval_vector_set_binary,
    eval_vector_vector_binary, is_result_metadata_label, validate_binary_modifier,
    validate_set_modifier,
};

/// Combines two already-evaluated instant operands into the binary result.
///
/// This function applies the operator and the modifier of `binary`. It is the
/// shared core of `PromQL` binary evaluation. The interpreter function
/// `PromqlEngine::eval_instant_binary` evaluates both operands through the
/// interpreter and then calls this function. The operator path
/// `PromqlEngine::plan_binary_expr` recurses both operands through the planner,
/// assembles each one to an [`InstantValue`], and then calls this same function.
///
/// Both callers send their operands through one combine routine, so the two
/// paths are byte-for-byte identical once their operand vectors match. This
/// function decides the set operations, the vector matching, the `__name__`
/// dropping, the `bool` modifier, and the `group_left` and `group_right`
/// copying. The call site decides none of them.
pub(crate) fn combine_instant_binary(
    binary: &BinaryExpr,
    operands: InstantOperands,
    time_ms: i64,
) -> Result<QueryResult> {
    let InstantOperands { lhs, rhs } = operands;
    let modifier = binary.modifier.as_ref();

    if let Some(op) = SetOp::from_token(binary.op) {
        validate_set_modifier(modifier)?;
        let (InstantValue::Vector(left), InstantValue::Vector(right)) = (lhs, rhs) else {
            return Err(PromqlError::Plan(format!(
                "set operator `{}` requires instant-vector operands",
                binary.op
            )));
        };
        return Ok(QueryResult::InstantVector(eval_vector_set_binary(
            left, right, op, modifier,
        )));
    }

    validate_binary_modifier(modifier)?;
    let op = BinaryOp::try_from_token(binary.op)?;
    match (lhs, rhs) {
        (InstantValue::Scalar(left), InstantValue::Scalar(right)) => {
            if matches!(op, BinaryOp::TrimUpper | BinaryOp::TrimLower) {
                return Err(PromqlError::Exec(format!(
                    "operator {:?} not allowed for Scalar operations",
                    op.symbol()
                )));
            }
            let Some(value) = op.apply_scalar(left, right, modifier) else {
                return Err(PromqlError::Plan(
                    "scalar comparison without bool cannot filter a scalar".to_string(),
                ));
            };
            Ok(QueryResult::Scalar {
                ts_ms: time_ms,
                value,
            })
        }
        (InstantValue::Vector(vector), InstantValue::Scalar(scalar)) => {
            let samples = vector
                .into_iter()
                .filter_map(|sample| {
                    op.apply_vector_scalar(sample, scalar, modifier, ScalarSide::Right)
                })
                .collect();
            Ok(QueryResult::InstantVector(samples))
        }
        (InstantValue::Scalar(scalar), InstantValue::Vector(vector)) => {
            let samples = vector
                .into_iter()
                .filter_map(|sample| {
                    op.apply_vector_scalar(sample, scalar, modifier, ScalarSide::Left)
                })
                .collect();
            Ok(QueryResult::InstantVector(samples))
        }
        (InstantValue::Vector(left), InstantValue::Vector(right)) => {
            let mut samples = eval_vector_vector_binary(
                VectorOperands { left, right },
                VectorMatching { op, modifier },
            )?;
            if op.changes_metric_schema() {
                // Unlike vector/scalar arithmetic, resultMetric clears schema
                // metadata eagerly. A subquery must not treat the left metric
                // as the origin of a newly computed vector/vector value.
                for sample in &mut samples {
                    sample.labels = sample
                        .labels
                        .iter()
                        .filter(|(name, _)| !is_result_metadata_label(name))
                        .map(|(name, value)| (name.clone(), value.clone()))
                        .collect();
                }
            }
            Ok(QueryResult::InstantVector(samples))
        }
    }
}
