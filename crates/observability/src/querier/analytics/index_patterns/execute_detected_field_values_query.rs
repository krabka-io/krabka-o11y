use super::{
    HeaderMap, HttpQueryError, QuerierState, Value, collect_detected_fields, json,
    parse_detected_fields_params,
};

pub(crate) async fn execute_detected_field_values_query(
    state: &QuerierState,
    headers: &HeaderMap,
    name: &str,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_detected_fields_params(raw_query)?;
    let limit = params.limit;
    let fields = collect_detected_fields(state, headers, &params).await?;
    let values = fields
        .get(name)
        .map(|stats| stats.values.iter().take(limit).cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    if values.is_empty() {
        return Ok(json!({}));
    }

    Ok(json!({
        "values": values,
        "limit": limit,
    }))
}
