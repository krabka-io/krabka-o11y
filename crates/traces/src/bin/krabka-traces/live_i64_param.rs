pub(crate) fn live_i64_param(uri: &axum::http::Uri, name: &str) -> Result<i64, String> {
    uri.query()
        .and_then(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.into_owned())
        })
        .ok_or_else(|| format!("missing query parameter {name}"))?
        .parse::<i64>()
        .map_err(|_| format!("invalid query parameter {name}"))
}
