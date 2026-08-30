use super::*;

pub(crate) async fn execute_detected_fields_query(
    state: &QuerierState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_detected_fields_params(raw_query)?;
    let limit = params.limit;
    let fields = collect_detected_fields(state, headers, &params).await?;
    let fields = fields
        .into_iter()
        .take(limit)
        .map(|(label, stats)| {
            let ty = stats.ty.as_loki_str();
            let cardinality = stats.values.len();
            let parsers = stats.parsers_json();
            json!({
                "label": label,
                "type": ty,
                "cardinality": cardinality,
                "parsers": parsers,
            })
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
