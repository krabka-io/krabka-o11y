use super::{HeaderMap, HttpQueryError, QuerierState, RequestSecurity, Value, json};

pub(crate) async fn execute_detected_field_values_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    name: &str,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let super::DetectedFieldsQuery { limit, fields } =
        super::detected_fields_query(state, security, headers, raw_query).await?;
    let values = fields
        .get(name)
        .map_or_default(|stats| stats.values.iter().take(limit).cloned().collect::<Vec<_>>());
    if values.is_empty() {
        return Ok(json!({}));
    }

    Ok(json!({
        "values": values,
        "limit": limit,
    }))
}
