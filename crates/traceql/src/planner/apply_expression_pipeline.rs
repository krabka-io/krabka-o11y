use std::collections::BTreeMap;

use arrow::{
    array::{BooleanArray, StringArray},
    compute::filter_record_batch,
    record_batch::RecordBatch,
};

use crate::{
    ast::{Aggregate, ComparisonOp, Pipeline, ScalarAggregate, ScalarExpr, Value},
    engine::{CompareRow, compare_row, field_values, scalar_expression_value, scalar_matches},
    error::{Result, TraceqlError},
    ids::UnixNano,
    result::AttrValue,
    span_columns::COL_TRACE_ID,
};

pub(crate) fn requires_expression_execution(stage: &Pipeline) -> bool {
    matches!(
        stage,
        Pipeline::Group(_)
            | Pipeline::By(_)
            | Pipeline::Aggregate(
                Aggregate::Expression { .. }
                    | Aggregate::Count
                    | Aggregate::Sum(_)
                    | Aggregate::Avg(_)
                    | Aggregate::Min(_)
                    | Aggregate::Max(_)
            )
    )
}

pub(crate) fn uses_expression_pipeline(pipeline: &[Pipeline]) -> bool {
    pipeline.iter().any(requires_expression_execution)
        && (!pipeline
            .iter()
            .any(|stage| matches!(stage, Pipeline::TopK(_) | Pipeline::BottomK(_)))
            || pipeline.iter().any(|stage| {
                matches!(
                    stage,
                    Pipeline::Group(_) | Pipeline::Aggregate(Aggregate::Expression { .. })
                )
            }))
}

struct Spanset {
    rows: Vec<usize>,
    scalar: Option<Value>,
    attributes: Vec<(String, AttrValue)>,
}

/// Evaluates the supported scalar pipeline in order, with one initial spanset
/// per trace. Expression operands use the same typed evaluator as selectors.
pub(crate) fn apply_expression_pipeline(
    batches: &[RecordBatch],
    pipeline: &[Pipeline],
) -> Result<Vec<RecordBatch>> {
    let mut rows = Vec::new();
    let mut traces = BTreeMap::<String, Vec<usize>>::new();
    for batch in batches {
        for index in 0..batch.num_rows() {
            let row = compare_row(batch, index, UnixNano(0))?;
            let trace = row.columns.get(COL_TRACE_ID).ok_or_else(|| {
                TraceqlError::Exec("scalar pipeline requires trace identity".into())
            })?;
            traces
                .entry(format!("{trace:?}"))
                .or_default()
                .push(rows.len());
            rows.push(row);
        }
    }
    let mut sets = traces
        .into_values()
        .map(|rows| Spanset {
            rows,
            scalar: None,
            attributes: Vec::new(),
        })
        .collect::<Vec<_>>();
    // Preserve the pre-existing adjacent `aggregate() by(fields)` extension:
    // its grouping is part of the aggregate, unlike a later independent by().
    let mut stages = pipeline.iter().collect::<Vec<_>>();
    for index in 0..stages.len().saturating_sub(1) {
        if matches!(stages[index], Pipeline::Aggregate(_))
            && matches!(stages[index + 1], Pipeline::By(_))
        {
            stages.swap(index, index + 1);
        }
    }
    for stage in stages {
        match stage {
            Pipeline::Group(expr) => sets = regroup(sets, &rows, &[expr])?,
            Pipeline::By(fields) => {
                let exprs = fields
                    .iter()
                    .cloned()
                    .map(ScalarExpr::Field)
                    .collect::<Vec<_>>();
                sets = regroup(sets, &rows, &exprs.iter().collect::<Vec<_>>())?;
            }
            Pipeline::Aggregate(aggregate) => {
                let mut output = Vec::new();
                for mut set in sets {
                    set.scalar = aggregate_value(aggregate, &set.rows, &rows)?;
                    if let Some(value) = &set.scalar {
                        set.attributes
                            .push((aggregate_name(aggregate), attribute_value(value)));
                        output.push(set);
                    }
                }
                sets = output;
            }
            Pipeline::Filter { op, value } => sets.retain(|set| {
                set.scalar
                    .as_ref()
                    .is_some_and(|scalar| scalar_matches(scalar, *op, &Value::Float(*value)))
            }),
            Pipeline::Coalesce => {
                let mut traces = BTreeMap::<String, Vec<usize>>::new();
                for set in sets {
                    for index in set.rows {
                        let trace = &rows[index].columns[COL_TRACE_ID];
                        traces.entry(format!("{trace:?}")).or_default().push(index);
                    }
                }
                sets = traces
                    .into_values()
                    .map(|rows| Spanset {
                        rows,
                        scalar: None,
                        attributes: Vec::new(),
                    })
                    .collect();
            }
            Pipeline::Select(_) | Pipeline::With(_) => {}
            other => {
                return Err(TraceqlError::Unsupported(format!(
                    "scalar expression pipeline stage {other:?}"
                )));
            }
        }
    }
    let mut admitted = vec![false; rows.len()];
    let mut group_ids = vec![String::new(); rows.len()];
    let mut attributes = vec![String::new(); rows.len()];
    for (group_id, set) in sets.into_iter().enumerate() {
        let encoded = serde_json::to_string(&set.attributes)
            .map_err(|error| TraceqlError::Exec(error.to_string()))?;
        for index in set.rows {
            admitted[index] = true;
            group_ids[index] = group_id.to_string();
            attributes[index].clone_from(&encoded);
        }
    }
    let mut offset = 0;
    let mut output = Vec::new();
    for batch in batches {
        let end = offset + batch.num_rows();
        let mask = BooleanArray::from(admitted[offset..end].to_vec());
        let mut fields = batch.schema().fields().to_vec();
        let mut columns = batch.columns().to_vec();
        fields.push(std::sync::Arc::new(arrow::datatypes::Field::new(
            "__traceql.spanset",
            arrow::datatypes::DataType::Utf8,
            false,
        )));
        columns.push(std::sync::Arc::new(StringArray::from(
            group_ids[offset..end].to_vec(),
        )));
        fields.push(std::sync::Arc::new(arrow::datatypes::Field::new(
            "__traceql.spanset_attributes",
            arrow::datatypes::DataType::Utf8,
            false,
        )));
        columns.push(std::sync::Arc::new(StringArray::from(
            attributes[offset..end].to_vec(),
        )));
        let grouped = RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(fields)),
            columns,
        )
        .map_err(|error| TraceqlError::Exec(error.to_string()))?;
        output.push(
            filter_record_batch(&grouped, &mask)
                .map_err(|error| TraceqlError::Exec(error.to_string()))?,
        );
        offset = end;
    }
    // Retain the original schema even when filtering admits no spans.
    Ok(output)
}

fn regroup(sets: Vec<Spanset>, rows: &[CompareRow], exprs: &[&ScalarExpr]) -> Result<Vec<Spanset>> {
    let mut output = Vec::new();
    for set in sets {
        let mut groups = BTreeMap::<String, Spanset>::new();
        for index in set.rows {
            let values = exprs
                .iter()
                .map(|expr| {
                    if let ScalarExpr::Field(field) = expr {
                        Ok(field_values(field, &rows[index]))
                    } else {
                        Ok(vec![scalar_expression_value(expr, &rows[index])?])
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let group = groups.entry(format!("{values:?}")).or_insert_with(|| {
                let mut attributes = set.attributes.clone();
                for (expr, values) in exprs.iter().zip(&values) {
                    let name = format!("by({})", super::expression_name::scalar_name(expr));
                    if values.is_empty() {
                        attributes.push((name, attribute_value(&Value::Nil)));
                    } else {
                        for value in values {
                            attributes.push((name.clone(), group_attribute_value(expr, value)));
                        }
                    }
                }
                Spanset {
                    rows: Vec::new(),
                    scalar: None,
                    attributes,
                }
            });
            group.rows.push(index);
        }
        output.extend(groups.into_values());
    }
    Ok(output)
}

fn group_attribute_value(expr: &ScalarExpr, value: &Value) -> AttrValue {
    if let (ScalarExpr::Field(field), Value::Int(value)) = (expr, value) {
        match field.scope {
            crate::ast::Scope::Intrinsic(crate::ast::Intrinsic::Kind) => {
                return AttrValue::Str(
                    crate::engine::kind_enum_name(
                        i32::try_from(*value).expect("intrinsic enum is an i32"),
                    )
                    .into(),
                );
            }
            crate::ast::Scope::Intrinsic(crate::ast::Intrinsic::Status) => {
                return AttrValue::Str(
                    crate::engine::status_enum_name(
                        i32::try_from(*value).expect("intrinsic enum is an i32"),
                    )
                    .into(),
                );
            }
            _ => {}
        }
    }
    attribute_value(value)
}

fn attribute_value(value: &Value) -> AttrValue {
    match value {
        Value::Array(values) => AttrValue::Array(values.iter().map(attribute_value).collect()),
        Value::Str(value) => AttrValue::Str(value.clone()),
        Value::Int(value) => AttrValue::Int(*value),
        Value::Float(value) => AttrValue::Float(*value),
        Value::Bool(value) => AttrValue::Bool(*value),
        Value::Duration(value) => AttrValue::Str(crate::engine::metric_duration_label(*value)),
        Value::Nil => AttrValue::Str("nil".into()),
    }
}

fn aggregate_name(aggregate: &Aggregate) -> String {
    let (name, expr) = match aggregate {
        Aggregate::Count => return "count()".into(),
        Aggregate::Expression { function, expr } => (
            match function {
                ScalarAggregate::Sum => "sum",
                ScalarAggregate::Avg => "avg",
                ScalarAggregate::Min => "min",
                ScalarAggregate::Max => "max",
            },
            expr.clone(),
        ),
        Aggregate::Sum(field) => ("sum", ScalarExpr::Field(field.clone())),
        Aggregate::Avg(field) => ("avg", ScalarExpr::Field(field.clone())),
        Aggregate::Min(field) => ("min", ScalarExpr::Field(field.clone())),
        Aggregate::Max(field) => ("max", ScalarExpr::Field(field.clone())),
        _ => unreachable!("aggregate_value rejects non-scalar aggregates"),
    };
    format!("{name}({})", super::expression_name::scalar_name(&expr))
}

fn aggregate_value(
    aggregate: &Aggregate,
    indexes: &[usize],
    rows: &[CompareRow],
) -> Result<Option<Value>> {
    let (function, expr) = match aggregate {
        Aggregate::Count => {
            return Ok(Some(Value::Int(
                i64::try_from(indexes.len())
                    .map_err(|error| TraceqlError::Exec(error.to_string()))?,
            )));
        }
        Aggregate::Expression { function, expr } => (*function, expr.clone()),
        Aggregate::Sum(field) => (ScalarAggregate::Sum, ScalarExpr::Field(field.clone())),
        Aggregate::Avg(field) => (ScalarAggregate::Avg, ScalarExpr::Field(field.clone())),
        Aggregate::Min(field) => (ScalarAggregate::Min, ScalarExpr::Field(field.clone())),
        Aggregate::Max(field) => (ScalarAggregate::Max, ScalarExpr::Field(field.clone())),
        other => {
            return Err(TraceqlError::Unsupported(format!(
                "scalar aggregate {other:?}"
            )));
        }
    };
    let values = indexes
        .iter()
        .map(|index| scalar_expression_value(&expr, &rows[*index]))
        .collect::<Result<Vec<_>>>()?;
    Ok(reduce_values(function, &values))
}

fn reduce_values(function: ScalarAggregate, values: &[Value]) -> Option<Value> {
    let values = values
        .iter()
        .filter(|value| !matches!(value, Value::Nil))
        .collect::<Vec<_>>();
    let first = values.first()?;
    let mut result = (*first).clone();
    for value in &values[1..] {
        match function {
            ScalarAggregate::Sum | ScalarAggregate::Avg => match (&mut result, value) {
                (Value::Int(sum), Value::Int(value))
                | (Value::Duration(sum), Value::Duration(value)) => *sum = sum.wrapping_add(*value),
                (Value::Float(sum), Value::Float(value)) => *sum += *value,
                // Pinned Static.sumInto ignores values of a different type.
                _ => {}
            },
            ScalarAggregate::Min | ScalarAggregate::Max => {
                let op = if function == ScalarAggregate::Min {
                    ComparisonOp::Lt
                } else {
                    ComparisonOp::Gt
                };
                if scalar_matches(value, op, &result) {
                    result = (*value).clone();
                }
            }
        }
    }
    if function == ScalarAggregate::Avg {
        let count = values.len().to_string().parse::<f64>().ok()?;
        result = match result {
            // Pinned Static.divideBy reads the int's uint64 storage directly.
            Value::Int(value) => Value::Float(
                u64::from_ne_bytes(value.to_ne_bytes())
                    .to_string()
                    .parse::<f64>()
                    .ok()?
                    / count,
            ),
            Value::Duration(value) => Value::Duration(value / i64::try_from(values.len()).ok()?),
            Value::Float(value) => Value::Float(value / count),
            other => other,
        };
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{ScalarAggregate, Value, group_attribute_value, reduce_values};

    #[test]
    fn reductions_preserve_integer_wrapping_duration_average_and_first_runtime_type() {
        assert!(
            reduce_values(ScalarAggregate::Sum, &[Value::Int(i64::MAX), Value::Int(1)])
                == Some(Value::Int(i64::MIN))
        );
        assert!(
            reduce_values(
                ScalarAggregate::Avg,
                &[Value::Duration(5), Value::Duration(8)]
            ) == Some(Value::Duration(6))
        );
        assert!(
            reduce_values(
                ScalarAggregate::Sum,
                &[Value::Int(2), Value::Float(100.0), Value::Int(3)]
            ) == Some(Value::Int(5))
        );
        assert!(reduce_values(ScalarAggregate::Sum, &[Value::Nil]) == None);
    }

    #[test]
    fn enum_group_values_are_strings_while_integer_attributes_remain_integers() {
        use crate::{
            ast::{Field, Intrinsic, ScalarExpr, Scope},
            result::AttrValue,
        };

        for (intrinsic, code, name) in [
            (Intrinsic::Kind, 2, "server"),
            (Intrinsic::Status, 2, "error"),
        ] {
            let expr = ScalarExpr::Field(Field {
                scope: Scope::Intrinsic(intrinsic),
                key: String::new(),
            });
            assert!(group_attribute_value(&expr, &Value::Int(code)) == AttrValue::Str(name.into()));
            assert!(group_attribute_value(&expr, &Value::Int(code)) != AttrValue::Int(code));
        }
        let expr = ScalarExpr::Field(Field {
            scope: Scope::Span,
            key: "code".into(),
        });
        assert!(group_attribute_value(&expr, &Value::Int(i64::MAX)) == AttrValue::Int(i64::MAX));
    }
}
