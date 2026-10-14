use super::{HeaderMap, HttpQueryError, QuerierState, RequestSecurity, Value, json};

pub(crate) async fn execute_detected_fields_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let super::DetectedFieldsQuery { limit, fields } =
        super::detected_fields_query(state, security, headers, raw_query).await?;
    let fields = fields
        .into_iter()
        .take(limit)
        .map(|field_entry| {
            let label = field_entry.0;
            let field_stats = field_entry.1;
            let ty = field_stats.ty.as_loki_str();
            let cardinality = field_stats.values.len();
            // Loki names the JSON path a `json`-parsed field was read from, so
            // a client can build a `| json name="path"` stage from it. Only
            // top-level keys are detected here, so the path is the key.
            let json_path = field_stats.parsers.contains("json").then(|| json!([label]));
            let parsers = field_stats.parsers_json();
            let mut field = json!({
                "label": label,
                "type": ty,
                "cardinality": cardinality,
                "parsers": parsers,
            });
            if let Some(json_path) = json_path {
                field["jsonPath"] = json_path;
            }
            field
        })
        .collect::<Vec<_>>();
    if fields.is_empty() {
        return Ok(json!({}));
    }

    Ok(json!({
        "fields": fields,
        "limit": limit,
    }))
}
