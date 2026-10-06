use super::{
    ATTR_PREFIX, AttrValue, DataType, INSTRUMENTATION_ATTR_PREFIX, MatchScope, MatchValue,
    RESOURCE_ATTR_PREFIX, RecordBatch, SpanMatcher, TraceqlError, add_span_attr_columns_to_batch,
    attr_values_with_resource, span_schema,
};

/// Materialize the regular span and resource attribute columns, `attr.<key>`,
/// that metric `by()` and `select()` projections reference.
///
/// The selector path filters attributes directly on the parquet arrays, so
/// nothing else builds these columns, and `rate() by(span.http.method)` cannot
/// `GROUP BY attr.http.method` without them.
///
/// Literal comparisons supply a type even when every span lacks the attribute.
/// Otherwise the stored values determine the type, so numeric aggregates can
/// read native numbers. A missing or differently typed value becomes NULL.
///
/// `add_nested_intrinsic_columns` handles the Event and Link matchers. This
/// function skips `service.name`, which is the promoted
/// `COL_ROOT_SERVICE_NAME` column rather than an attribute.
pub(crate) fn add_span_attr_columns(
    mut batches: Vec<RecordBatch>,
    projection_matchers: &[SpanMatcher],
) -> Result<Vec<RecordBatch>, TraceqlError> {
    // (column_name, attr-array lookup key, include_resource, optional literal type).
    let mut wanted: Vec<(String, String, bool, Option<DataType>)> = Vec::new();
    for matcher in projection_matchers {
        let (lookup_key, include_resource) = match matcher.scope {
            MatchScope::Span | MatchScope::Both => (matcher.key.clone(), false),
            MatchScope::Resource => (format!("{RESOURCE_ATTR_PREFIX}{}", matcher.key), true),
            MatchScope::Instrumentation => (
                format!("{INSTRUMENTATION_ATTR_PREFIX}{}", matcher.key),
                false,
            ),
            _ => continue,
        };
        if matcher.key == "service.name" {
            continue; // grouped via the promoted COL_ROOT_SERVICE_NAME column
        }
        let column_name = if matcher.scope == MatchScope::Instrumentation {
            format!("{ATTR_PREFIX}{INSTRUMENTATION_ATTR_PREFIX}{}", matcher.key)
        } else {
            format!("{ATTR_PREFIX}{}", matcher.key)
        };
        let hint = match matcher.value {
            MatchValue::Str(_) => Some(DataType::Utf8),
            MatchValue::Int(_) => Some(DataType::Int64),
            MatchValue::Float(_) => Some(DataType::Float64),
            MatchValue::Bool(_) => Some(DataType::Boolean),
            MatchValue::Nil => None,
        };
        if let Some((_, _, _, existing)) = wanted
            .iter_mut()
            .find(|(name, _, _, _)| name == &column_name)
        {
            if let Some(next) = hint {
                *existing = Some(match existing.as_ref() {
                    Some(previous) => merge_projection_types(&column_name, previous, next)?,
                    None => next,
                });
            }
        } else {
            wanted.push((column_name, lookup_key, include_resource, hint));
        }
    }
    if wanted.is_empty() {
        return Ok(batches);
    }
    let mut typed = Vec::with_capacity(wanted.len());
    for (column_name, lookup_key, include_resource, hint) in wanted {
        let data_type = if let Some(hint) = hint {
            hint
        } else {
            let mut inferred = None;
            for batch in &batches {
                for row in 0..batch.num_rows() {
                    if let Some((_, value)) =
                        attr_values_with_resource(batch, row, include_resource)?
                            .into_iter()
                            .find(|(key, _)| key == &lookup_key)
                    {
                        let next = match value {
                            AttrValue::Str(_) => DataType::Utf8,
                            AttrValue::Int(_) => DataType::Int64,
                            AttrValue::Float(_) => DataType::Float64,
                            AttrValue::Bool(_) => DataType::Boolean,
                        };
                        inferred = Some(match inferred.as_ref() {
                            Some(previous) => merge_projection_types(&column_name, previous, next)?,
                            None => next,
                        });
                    }
                }
            }
            inferred.unwrap_or(DataType::Utf8)
        };
        typed.push((column_name, lookup_key, include_resource, data_type));
    }
    // A schema-only batch keeps valid empty-store queries plannable, including
    // comparisons against absent numeric/boolean attributes.
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(span_schema()));
    }
    batches
        .into_iter()
        .map(|batch| add_span_attr_columns_to_batch(&batch, &typed))
        .collect()
}

fn merge_projection_types(
    column_name: &str,
    previous: &DataType,
    next: DataType,
) -> Result<DataType, TraceqlError> {
    if previous == &next {
        return Ok(next);
    }
    if matches!(
        (previous, &next),
        (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64)
    ) {
        return Ok(DataType::Float64);
    }
    Err(TraceqlError::Store(format!(
        "incompatible projection types for {column_name}: {previous:?} and {next:?}"
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array};
    use assert2::assert;
    use datafusion::{catalog::MemTable, prelude::SessionContext};
    use krabka_traceql::MatchCmp;

    use super::*;
    use crate::span::{
        AttrValue as SpanAttrValue, KeyValue, Span, SpanKind, StatusCode, batch::span_batch,
    };

    fn projection(key: &str, value: MatchValue) -> SpanMatcher {
        SpanMatcher {
            scope: MatchScope::Span,
            key: key.into(),
            op: MatchCmp::Eq,
            value,
            negated: false,
        }
    }

    fn packed(attrs: Vec<Vec<(&str, SpanAttrValue)>>) -> RecordBatch {
        let spans = attrs
            .into_iter()
            .enumerate()
            .map(|(index, attrs)| Span {
                trace_id: [1; 16],
                span_id: [u8::try_from(index + 1).unwrap(); 8],
                parent_span_id: None,
                name: "operation".into(),
                kind: SpanKind::Server,
                start_ns: 1_000,
                duration_ns: 500,
                status: StatusCode::Ok,
                status_message: String::new(),
                resource_attrs: Vec::new(),
                span_attrs: attrs
                    .into_iter()
                    .map(|(key, value)| KeyValue {
                        key: key.into(),
                        value,
                    })
                    .collect(),
                events: Vec::new(),
                links: Vec::new(),
                instrumentation_scope: String::new(),
                instrumentation_version: String::new(),
            })
            .collect::<Vec<_>>();
        span_batch(&spans).expect("packed span fixture")
    }

    async fn sql(batches: Vec<RecordBatch>, query: &str) -> Vec<RecordBatch> {
        let ctx = SessionContext::new();
        let table = MemTable::try_new(batches[0].schema(), vec![batches]).unwrap();
        ctx.register_table("spans", Arc::new(table)).unwrap();
        ctx.sql(query).await.unwrap().collect().await.unwrap()
    }

    #[tokio::test]
    async fn packed_typed_values_support_comparisons_without_string_coercion() {
        let batch = packed(vec![
            vec![
                ("attempts", SpanAttrValue::Int(3)),
                ("load", SpanAttrValue::Double(3.5)),
                ("ready", SpanAttrValue::Bool(true)),
                ("db.system", SpanAttrValue::Str("postgresql".into())),
            ],
            vec![
                ("attempts", SpanAttrValue::Int(7)),
                ("load", SpanAttrValue::Double(7.5)),
                ("ready", SpanAttrValue::Bool(true)),
                ("db.system", SpanAttrValue::Str("postgresql".into())),
            ],
            vec![],
            vec![("attempts", SpanAttrValue::Str("7".into()))],
        ]);
        let batches = add_span_attr_columns(
            vec![batch],
            &[
                projection("attempts", MatchValue::Int(3)),
                projection("load", MatchValue::Float(3.0)),
                projection("ready", MatchValue::Bool(true)),
                projection("db.system", MatchValue::Str("postgresql".into())),
            ],
        )
        .unwrap();
        let attempts = batches[0]
            .column_by_name("attr.attempts")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(attempts == &Int64Array::from(vec![Some(3), Some(7), None, None]));
        let result = sql(
            batches,
            "SELECT SUM(\"attr.attempts\") AS total, AVG(\"attr.load\") AS load \
             FROM spans WHERE \"attr.attempts\" > 3 AND \"attr.ready\" = true \
             AND \"attr.db.system\" = 'postgresql'",
        )
        .await;
        let total = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let load = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(total == &Int64Array::from(vec![7]));
        assert!(load == &Float64Array::from(vec![7.5]));
    }

    #[tokio::test]
    async fn unhinted_numeric_projections_infer_one_type_across_batches() {
        let batches = add_span_attr_columns(
            vec![
                packed(vec![vec![("attempts", SpanAttrValue::Int(3))]]),
                packed(vec![vec![], vec![("attempts", SpanAttrValue::Int(7))]]),
            ],
            &[projection("attempts", MatchValue::Nil)],
        )
        .unwrap();
        let result = sql(batches, "SELECT SUM(\"attr.attempts\") FROM spans").await;
        let total = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(total == &Int64Array::from(vec![10]));
    }

    #[tokio::test]
    async fn compatible_numeric_hints_and_batches_widen_to_float() {
        for projections in [
            vec![
                projection("cost", MatchValue::Int(1)),
                projection("cost", MatchValue::Float(1.5)),
            ],
            vec![
                projection("cost", MatchValue::Float(1.5)),
                projection("cost", MatchValue::Int(1)),
            ],
            vec![projection("cost", MatchValue::Nil)],
        ] {
            for reverse in [false, true] {
                let mut packed_batches = vec![
                    packed(vec![
                        vec![("cost", SpanAttrValue::Int(1))],
                        vec![("cost", SpanAttrValue::Int(2))],
                    ]),
                    packed(vec![
                        vec![("cost", SpanAttrValue::Double(1.25))],
                        vec![("cost", SpanAttrValue::Double(1.75))],
                    ]),
                    packed(vec![vec![]]),
                ];
                if reverse {
                    packed_batches.reverse();
                }
                let batches = add_span_attr_columns(packed_batches, &projections).unwrap();
                for batch in &batches {
                    assert!(
                        batch.column_by_name("attr.cost").unwrap().data_type()
                            == &DataType::Float64
                    );
                }
                let all = sql(batches.clone(), "SELECT SUM(\"attr.cost\") FROM spans").await;
                let total = all[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                // Includes both integer values; dropping them would return 3.
                assert!(total == &Float64Array::from(vec![6.0]));
                let selected = sql(
                    batches,
                    "SELECT \"attr.cost\" FROM spans \
                     WHERE \"attr.cost\" > 1 AND \"attr.cost\" < 1.5",
                )
                .await;
                let values = selected
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .iter()
                    })
                    .collect::<Vec<_>>();
                assert!(values == vec![Some(1.25)]);
            }
        }
    }

    #[tokio::test]
    async fn empty_store_exposes_requested_typed_columns() {
        let batches = add_span_attr_columns(
            Vec::new(),
            &[
                projection("attempts", MatchValue::Int(3)),
                projection("load", MatchValue::Float(3.0)),
                projection("ready", MatchValue::Bool(true)),
                projection("db.system", MatchValue::Str("postgresql".into())),
            ],
        )
        .unwrap();
        assert!(batches.len() == 1);
        assert!(batches[0].num_rows() == 0);
        for (name, expected) in [
            ("attr.attempts", DataType::Int64),
            ("attr.load", DataType::Float64),
            ("attr.ready", DataType::Boolean),
            ("attr.db.system", DataType::Utf8),
        ] {
            let field = batches[0].schema().field_with_name(name).unwrap().clone();
            assert!(field.data_type() == &expected);
            assert!(field.is_nullable());
        }
        let result = sql(
            batches,
            "SELECT * FROM spans WHERE \"attr.attempts\" > 3 OR \
             (\"attr.load\" > 0.5 AND \"attr.ready\" = true) \
             OR \"attr.db.system\" = 'postgresql'",
        )
        .await;
        assert!(result.iter().map(RecordBatch::num_rows).sum::<usize>() == 0);
    }

    #[test]
    fn incompatible_unhinted_values_and_literal_hints_are_explicit_errors() {
        let batch = packed(vec![
            vec![("attempts", SpanAttrValue::Int(7))],
            vec![("attempts", SpanAttrValue::Str("7".into()))],
        ]);
        let mixed = add_span_attr_columns(vec![batch], &[projection("attempts", MatchValue::Nil)])
            .unwrap_err();
        assert!(mixed.to_string().contains("incompatible projection types"));
        let conflict = add_span_attr_columns(
            Vec::new(),
            &[
                projection("attempts", MatchValue::Int(7)),
                projection("attempts", MatchValue::Str("7".into())),
            ],
        )
        .unwrap_err();
        assert!(
            conflict
                .to_string()
                .contains("incompatible projection types")
        );
    }

    #[test]
    fn promoted_native_columns_are_preserved() {
        let batch = packed(vec![vec![]]);
        let promoted: super::super::ArrayRef = Arc::new(Int64Array::from(vec![7]));
        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(super::super::Field::new(
            "attr.attempts",
            DataType::Int64,
            true,
        ));
        let mut columns = batch.columns().to_vec();
        columns.push(promoted.clone());
        let batch =
            RecordBatch::try_new(Arc::new(super::super::Schema::new(fields)), columns).unwrap();
        let batches = add_span_attr_columns(
            vec![batch],
            &[projection("attempts", MatchValue::Float(7.0))],
        )
        .unwrap();
        assert!(Arc::ptr_eq(
            batches[0].column_by_name("attr.attempts").unwrap(),
            &promoted
        ));
    }
}
