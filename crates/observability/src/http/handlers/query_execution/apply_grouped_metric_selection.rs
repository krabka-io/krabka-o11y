use std::collections::BTreeMap;

use crate::{
    HttpQueryError, Labels, Value, VectorAggregation, VectorAggregationOp, apply_metric_selection,
    json, metric_series_labels, querier::aggregate::sample_windows::vector_group_labels,
};

// A selection keeps full sample labels, while its grouping chooses independent
// ranking buckets. The existing selector also ranks matrix points per timestamp.
pub(crate) fn apply_grouped_metric_selection(
    value: &mut Value,
    aggregation: &VectorAggregation,
) -> Result<(), HttpQueryError> {
    let (limit, largest) = match aggregation.op {
        VectorAggregationOp::TopK(limit) => (limit, true),
        VectorAggregationOp::BottomK(limit) => (limit, false),
        _ => {
            return Err(HttpQueryError::VariantUnsupported(
                "expected topk or bottomk".into(),
            ));
        }
    };
    let mut groups = BTreeMap::<Labels, Vec<Value>>::new();
    let rows = value["data"]["result"]
        .as_array_mut()
        .expect("metric result is an array");
    for row in std::mem::take(rows) {
        let labels = metric_series_labels(&row)
            .ok_or_else(|| HttpQueryError::VariantUnsupported("invalid metric labels".into()))?;
        groups
            .entry(vector_group_labels(&labels, aggregation.grouping.as_ref()))
            .or_default()
            .push(row);
    }
    let mut selected = Vec::new();
    for rows in groups.into_values() {
        let mut group = json!({"data":{"resultType":value["data"]["resultType"],"result":rows}});
        apply_metric_selection(
            &mut group,
            usize::try_from(limit).unwrap_or(usize::MAX),
            largest,
        );
        selected.extend(
            group["data"]["result"]
                .as_array_mut()
                .map(std::mem::take)
                .unwrap_or_default(),
        );
    }
    value["data"]["result"] = Value::Array(selected);
    Ok(())
}
