use super::{
    HttpQueryError, LogqlExpr, LokiDirection, LokiStreamEncoding, QuerierState, QueryKind,
    ScalarVectorExpressionResult, TimeRange, Value, add_loki_query_stats, apply_label_join_fields,
    apply_label_replace_to_loki_result, apply_metric_binary_arithmetic_to_loki_result,
    apply_metric_binary_comparison_to_loki_result, apply_metric_binary_set_to_loki_result,
    apply_metric_selection, apply_scalar_arithmetic_to_loki_result,
    apply_scalar_comparison_to_loki_result, execute_http_metric_query, execute_http_stream_query,
    loki_instant_scalar_or_vector_response, loki_range_vector_response, merge_loki_query_stats,
    resolved_range_step, retain_metric_binary_on_labels, scalar_vector_expression_result,
    sort_loki_vector_result,
};

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
pub(crate) async fn execute_http_logql_expr(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    expression: &LogqlExpr,
    stream_options: (LokiDirection, Option<usize>, Option<i64>),
    encoding: LokiStreamEncoding,
    full_query: &str,
) -> Result<Value, HttpQueryError> {
    match expression {
        LogqlExpr::Stream { source, .. } => {
            let (direction, limit, interval) = stream_options;
            execute_http_stream_query(
                state,
                source,
                tenant,
                time_range,
                (
                    direction,
                    limit,
                    interval,
                    matches!(kind, QueryKind::Range).then_some(time_range.end_ns),
                ),
                encoding,
            )
            .await
            .map(add_loki_query_stats)
        }
        LogqlExpr::Metric { query, .. } => {
            execute_http_metric_query(state, tenant, time_range, step, kind, query.clone()).await
        }
        LogqlExpr::Scalar(_) | LogqlExpr::Vector(_) => {
            let result =
                scalar_vector_expression_result(&expression.to_string()).ok_or_else(|| {
                    HttpQueryError::LokiFormatPlainParse("invalid scalar expression".to_string())
                })?;
            Ok(add_loki_query_stats(match kind {
                QueryKind::Instant => {
                    loki_instant_scalar_or_vector_response(time_range.end_ns, result)
                }
                QueryKind::Range => loki_range_vector_response(
                    time_range,
                    resolved_range_step(step, time_range)?,
                    result,
                ),
            }))
        }
        LogqlExpr::Sort { expr, descending } => {
            let mut value = Box::pin(execute_http_logql_expr(
                state,
                tenant,
                time_range,
                step,
                kind,
                expr,
                stream_options,
                encoding,
                full_query,
            ))
            .await?;
            sort_loki_vector_result(&mut value, *descending);
            Ok(value)
        }
        LogqlExpr::Selection {
            expr,
            limit,
            largest,
            ..
        } => {
            let mut value = Box::pin(execute_http_logql_expr(
                state,
                tenant,
                time_range,
                step,
                kind,
                expr,
                stream_options,
                encoding,
                full_query,
            ))
            .await?;
            apply_metric_selection(
                &mut value,
                usize::try_from(*limit).unwrap_or(usize::MAX),
                *largest,
            );
            Ok(value)
        }
        LogqlExpr::LabelReplace {
            expr,
            destination_label,
            replacement,
            source_label,
            pattern,
        } => {
            let mut value = Box::pin(execute_http_logql_expr(
                state,
                tenant,
                time_range,
                step,
                kind,
                expr,
                stream_options,
                encoding,
                full_query,
            ))
            .await?;
            apply_label_replace_to_loki_result(
                &mut value,
                destination_label,
                replacement,
                source_label,
                pattern,
                full_query,
            )?;
            Ok(value)
        }
        LogqlExpr::LabelJoin {
            expr,
            destination_label,
            separator,
            source_labels,
        } => {
            let mut value = Box::pin(execute_http_logql_expr(
                state,
                tenant,
                time_range,
                step,
                kind,
                expr,
                stream_options,
                encoding,
                full_query,
            ))
            .await?;
            apply_label_join_fields(&mut value, destination_label, separator, source_labels);
            Ok(value)
        }
        LogqlExpr::Arithmetic {
            left,
            op,
            matching,
            right,
        } => {
            if let Some(value) = scalar_expression_response(expression, time_range, step, kind)? {
                return Ok(value);
            }
            if let Some((scalar, scalar_on_left, vector)) = scalar_operand(left, right) {
                let mut value = Box::pin(execute_http_logql_expr(
                    state,
                    tenant,
                    time_range,
                    step,
                    kind,
                    vector,
                    stream_options,
                    encoding,
                    full_query,
                ))
                .await?;
                apply_scalar_arithmetic_to_loki_result(
                    &mut value,
                    *op,
                    &scalar,
                    scalar_on_left,
                    full_query,
                )?;
                return Ok(value);
            }
            let left_is_vector = is_scalar_vector_only(left);
            let right_is_vector = is_scalar_vector_only(right);
            let mut left = Box::pin(execute_http_logql_expr(
                state,
                tenant,
                time_range,
                step,
                kind,
                left,
                stream_options,
                encoding,
                full_query,
            ))
            .await?;
            let right = Box::pin(execute_http_logql_expr(
                state,
                tenant,
                time_range,
                step,
                kind,
                right,
                stream_options,
                encoding,
                full_query,
            ))
            .await?;
            apply_metric_binary_arithmetic_to_loki_result(
                &mut left,
                &right,
                *op,
                matching.as_ref(),
            );
            if left_is_vector || right_is_vector {
                retain_metric_binary_on_labels(&mut left, matching.as_ref());
                if left_is_vector && !right_is_vector {
                    merge_loki_query_stats(&mut left["data"]["stats"], &right["data"]["stats"]);
                }
            }
            Ok(left)
        }
        LogqlExpr::Comparison {
            left,
            op,
            bool_modifier,
            matching,
            right,
        } => {
            if let Some(value) = scalar_expression_response(expression, time_range, step, kind)? {
                return Ok(value);
            }
            if let Some((scalar, scalar_on_left, vector)) = scalar_operand(left, right) {
                let mut value = Box::pin(execute_http_logql_expr(
                    state,
                    tenant,
                    time_range,
                    step,
                    kind,
                    vector,
                    stream_options,
                    encoding,
                    full_query,
                ))
                .await?;
                apply_scalar_comparison_to_loki_result(
                    &mut value,
                    *op,
                    *bool_modifier,
                    &scalar,
                    scalar_on_left,
                    full_query,
                )?;
                return Ok(value);
            }
            let left_is_vector = is_scalar_vector_only(left);
            let right_is_vector = is_scalar_vector_only(right);
            let mut left = Box::pin(execute_http_logql_expr(
                state,
                tenant,
                time_range,
                step,
                kind,
                left,
                stream_options,
                encoding,
                full_query,
            ))
            .await?;
            let right = Box::pin(execute_http_logql_expr(
                state,
                tenant,
                time_range,
                step,
                kind,
                right,
                stream_options,
                encoding,
                full_query,
            ))
            .await?;
            apply_metric_binary_comparison_to_loki_result(
                &mut left,
                &right,
                *op,
                *bool_modifier,
                matching.as_ref(),
            );
            if left_is_vector || right_is_vector {
                retain_metric_binary_on_labels(&mut left, matching.as_ref());
                if left_is_vector && !right_is_vector {
                    merge_loki_query_stats(&mut left["data"]["stats"], &right["data"]["stats"]);
                }
            }
            Ok(left)
        }
        LogqlExpr::Set {
            left,
            op,
            matching,
            right,
        } => {
            let left_is_vector = is_scalar_vector_only(left);
            let right_is_vector = is_scalar_vector_only(right);
            let mut left = Box::pin(execute_http_logql_expr(
                state,
                tenant,
                time_range,
                step,
                kind,
                left,
                stream_options,
                encoding,
                full_query,
            ))
            .await?;
            let right = Box::pin(execute_http_logql_expr(
                state,
                tenant,
                time_range,
                step,
                kind,
                right,
                stream_options,
                encoding,
                full_query,
            ))
            .await?;
            apply_metric_binary_set_to_loki_result(&mut left, &right, *op, matching.as_ref());
            if left_is_vector && !right_is_vector {
                merge_loki_query_stats(&mut left["data"]["stats"], &right["data"]["stats"]);
            }
            Ok(left)
        }
    }
}

fn is_scalar_vector_only(expression: &LogqlExpr) -> bool {
    match expression {
        LogqlExpr::Scalar(_) | LogqlExpr::Vector(_) => true,
        LogqlExpr::Sort { expr, .. }
        | LogqlExpr::Selection { expr, .. }
        | LogqlExpr::LabelReplace { expr, .. }
        | LogqlExpr::LabelJoin { expr, .. } => is_scalar_vector_only(expr),
        LogqlExpr::Arithmetic { left, right, .. }
        | LogqlExpr::Comparison { left, right, .. }
        | LogqlExpr::Set { left, right, .. } => {
            is_scalar_vector_only(left) && is_scalar_vector_only(right)
        }
        LogqlExpr::Stream { .. } | LogqlExpr::Metric { .. } => false,
    }
}

fn scalar_operand<'a>(
    left: &'a LogqlExpr,
    right: &'a LogqlExpr,
) -> Option<(String, bool, &'a LogqlExpr)> {
    scalar_sample(left)
        .map(|scalar| (scalar, true, right))
        .or_else(|| scalar_sample(right).map(|scalar| (scalar, false, left)))
}

fn scalar_sample(expression: &LogqlExpr) -> Option<String> {
    match scalar_vector_expression_result(&expression.to_string())? {
        ScalarVectorExpressionResult::Scalar { sample } => Some(sample),
        ScalarVectorExpressionResult::Vector { .. } => None,
    }
}

fn scalar_expression_response(
    expression: &LogqlExpr,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
) -> Result<Option<Value>, HttpQueryError> {
    let Some(result @ ScalarVectorExpressionResult::Scalar { .. }) =
        scalar_vector_expression_result(&expression.to_string())
    else {
        return Ok(None);
    };
    Ok(Some(add_loki_query_stats(match kind {
        QueryKind::Instant => loki_instant_scalar_or_vector_response(time_range.end_ns, result),
        QueryKind::Range => {
            loki_range_vector_response(time_range, resolved_range_step(step, time_range)?, result)
        }
    })))
}
