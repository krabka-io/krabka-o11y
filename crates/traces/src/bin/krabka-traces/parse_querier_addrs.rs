use super::*;

/// Parse `--querier-url` into the bare `host:port` addresses the
/// [`HttpQuerier`] pool dials. The flag holds comma-separated querier URLs, and
/// a scheme is allowed.
pub(crate) fn parse_querier_addrs(
    value: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let mut addrs = Vec::new();
    for raw in value.split(',').map(str::trim).filter(|v| !v.is_empty()) {
        let url = Url::parse(raw)?;
        let host = url
            .host_str()
            .ok_or_else(|| format!("querier url missing host: {raw}"))?;
        let addr = match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };
        addrs.push(addr);
    }
    if addrs.is_empty() {
        return Err(format!("no querier addresses parsed from {value:?}").into());
    }
    Ok(addrs)
}
