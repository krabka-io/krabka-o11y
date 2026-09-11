use krabka_traces::frontend::QuerierScheme;

use super::Url;

/// Parse `--querier-url` into the scheme and the bare `host:port` addresses
/// that the [`HttpQuerier`] pool dials.
///
/// The flag holds comma-separated querier URLs. Each URL is `http` or
/// `https`, and every URL has the same scheme, because the frontend dials the
/// whole pool with one client.
///
/// [`HttpQuerier`]: krabka_traces::frontend::HttpQuerier
pub(crate) fn parse_querier_addrs(
    value: &str,
) -> Result<(QuerierScheme, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    let mut scheme = None;
    let mut addrs = Vec::new();
    for raw in value.split(',').map(str::trim).filter(|v| !v.is_empty()) {
        let url = Url::parse(raw)?;
        let this = match url.scheme() {
            "http" => QuerierScheme::Http,
            "https" => QuerierScheme::Https,
            other => {
                return Err(
                    format!("querier url {raw}: scheme {other:?} is not http or https").into(),
                );
            }
        };
        if scheme.is_some_and(|first| first != this) {
            return Err(format!("querier urls mix http and https: {value:?}").into());
        }
        scheme = Some(this);
        let host = url
            .host_str()
            .ok_or_else(|| format!("querier url missing host: {raw}"))?;
        let addr = match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };
        addrs.push(addr);
    }
    match scheme {
        Some(scheme) => Ok((scheme, addrs)),
        None => Err(format!("no querier addresses parsed from {value:?}").into()),
    }
}
