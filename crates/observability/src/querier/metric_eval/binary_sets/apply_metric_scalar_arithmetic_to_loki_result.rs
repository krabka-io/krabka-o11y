use super::*;

pub(crate) fn apply_metric_scalar_arithmetic_to_loki_result(
    value: &mut Value,
    arithmetic: &MetricScalarArithmetic,
    query: &str,
) -> Result<(), HttpQueryError> {
    let scalar =
        parse_metric_sample_value(&arithmetic.scalar).ok_or_else(|| HttpQueryError::LokiParse {
            query: query.to_string(),
            source: ParseError::Syntax {
                message: "expected scalar literal".to_string(),
                position: 0,
            },
        })?;
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return Ok(());
    };

    let mut index = 0;
    while index < results.len() {
        if apply_metric_scalar_arithmetic_to_series(
            &mut results[index],
            arithmetic.op,
            scalar,
            arithmetic.scalar_on_left,
        ) {
            index += 1;
        } else {
            results.remove(index);
        }
    }
    Ok(())
}
