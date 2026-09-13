use super::{
    HeaderMap, HttpQueryError, QuerierState, RequestSecurity, Value, collect_detected_fields, json,
    parse_detected_fields_params,
};

pub(crate) async fn execute_detected_fields_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_detected_fields_params(raw_query)?;
    let limit = params.limit;
    let fields = collect_detected_fields(state, security, headers, &params).await?;
    let fields = fields
        .into_iter()
        .take(limit)
        .map(|(label, stats)| {
            let ty = stats.ty.as_loki_str();
            let cardinality = stats.values.len();
            // Loki names the JSON path a `json`-parsed field was read from, so
            // a client can build a `| json name="path"` stage from it. Only
            // top-level keys are detected here, so the path is the key.
            let json_path = stats.parsers.contains("json").then(|| json!([label]));
            let parsers = stats.parsers_json();
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
