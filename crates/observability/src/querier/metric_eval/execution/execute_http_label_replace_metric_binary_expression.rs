use super::*;

pub(crate) async fn execute_http_label_replace_metric_binary_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    binary: LabelReplaceMetricBinaryExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    match binary {
        LabelReplaceMetricBinaryExpression::Arithmetic {
            left,
            op,
            matching,
            right,
        } => {
            let mut left = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &left, query_text,
            )
            .await?;
            let right = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &right, query_text,
            )
            .await?;
            apply_metric_binary_arithmetic_to_loki_result(&mut left, &right, op, matching.as_ref());
            retain_metric_binary_on_labels(&mut left, matching.as_ref());
            Ok(left)
        }
        LabelReplaceMetricBinaryExpression::Comparison {
            left,
            op,
            bool_modifier,
            matching,
            right,
        } => {
            let mut left = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &left, query_text,
            )
            .await?;
            let right = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &right, query_text,
            )
            .await?;
            apply_metric_binary_comparison_to_loki_result(
                &mut left,
                &right,
                op,
                bool_modifier,
                matching.as_ref(),
            );
            retain_metric_binary_on_labels(&mut left, matching.as_ref());
            Ok(left)
        }
        LabelReplaceMetricBinaryExpression::Set {
            left,
            op,
            matching,
            right,
        } => {
            let mut left = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &left, query_text,
            )
            .await?;
            let right = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &right, query_text,
            )
            .await?;
            apply_metric_binary_set_to_loki_result(&mut left, &right, op, matching.as_ref());
            Ok(left)
        }
    }
}
