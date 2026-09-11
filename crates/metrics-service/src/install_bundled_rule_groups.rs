use krabka_observability::server_security::ServerSecurity;

use super::{
    BUNDLED_RULES_HOST, BUNDLED_RULES_RESPONSE_MAX, BundledRuleFile, BundledRulesError,
    ByteSizeExt, SocketAddr, StdPath, TENANT_HEADER, TenantId, Url, bundled_group_label,
    bundled_rules_address, bundled_rules_namespace, header,
};

/// Installs every rule group of a bundled rule file through the ruler's own ruler-config API.
///
/// The ruler role calls this once at startup for `--ruler-bundled-rules`,
/// after its listener binds at `listener`. It posts each group over HTTP to
/// that listener, so a bundled group takes the same authentication,
/// validation, storage and audit as a group an operator posts. The file stem
/// names the rule namespace, and `/api/v1/rules` renders that namespace as the
/// rule file.
///
/// The requests go to [`BUNDLED_RULES_HOST`] on the listener's port, and that
/// name resolves to [`bundled_rules_address`] of `listener`. The HTTP client
/// takes its credentials from `security.internal_client()`. With
/// authentication on, those credentials should name a principal that may use
/// `tenant`. With TLS on, the internal client's CA should verify the server
/// certificate, and that certificate should be valid for `localhost`.
///
/// Returns the name of each installed group, in file order.
///
/// # Errors
///
/// Returns an error when the file is unreadable, when it is not a Prometheus
/// rule file, when it holds no rule group, when the HTTP client does not build,
/// when the listener address does not make a URL, when the request does not
/// reach the listener, or when the ruler config API rejects a group. An
/// operator who names a rule file and gets no rules has an alerting gap and no
/// signal, so each of these cases stops the start.
pub async fn install_bundled_rule_groups(
    listener: SocketAddr,
    security: &ServerSecurity,
    path: &StdPath,
    tenant: &TenantId,
) -> Result<Vec<String>, BundledRulesError> {
    let text = std::fs::read_to_string(path).map_err(|source| BundledRulesError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let file: BundledRuleFile =
        serde_yaml::from_str(&text).map_err(|source| BundledRulesError::Decode {
            path: path.to_path_buf(),
            source,
        })?;
    if file.groups.is_empty() {
        return Err(BundledRulesError::NoGroups {
            path: path.to_path_buf(),
        });
    }
    let namespace = bundled_rules_namespace(path)?;

    let client = security
        .internal_client()
        .apply(
            reqwest::Client::builder().resolve(BUNDLED_RULES_HOST, bundled_rules_address(listener)),
        )
        .build()
        .map_err(|source| BundledRulesError::Client { source })?;
    let scheme = if security.tls_enabled() {
        "https"
    } else {
        "http"
    };
    let address = format!("{scheme}://{BUNDLED_RULES_HOST}:{}/", listener.port());
    let mut url =
        Url::parse(&address).map_err(|source| BundledRulesError::Url { address, source })?;
    // An `http` or `https` URL always has path segments. Each segment is
    // percent-encoded, so any file stem names the namespace exactly.
    if let Ok(mut segments) = url.path_segments_mut() {
        segments
            .pop_if_empty()
            .extend(["prometheus", "config", "v1", "rules", namespace.as_str()]);
    }

    let mut installed = Vec::with_capacity(file.groups.len());
    for (index, group) in file.groups.iter().enumerate() {
        let label = bundled_group_label(index, group);
        let body = serde_yaml::to_string(group).map_err(|source| BundledRulesError::Encode {
            group: label.clone(),
            source,
        })?;
        let mut response = client
            .post(url.clone())
            .header(TENANT_HEADER, tenant.as_str())
            .header(header::CONTENT_TYPE, "application/yaml")
            .body(body)
            .send()
            .await
            .map_err(|source| BundledRulesError::Send {
                group: label.clone(),
                source,
            })?;
        let status = response.status();
        if !status.is_success() {
            let max = BUNDLED_RULES_RESPONSE_MAX.bytes_usize();
            let mut body = Vec::new();
            while body.len() < max
                && let Some(chunk) =
                    response
                        .chunk()
                        .await
                        .map_err(|source| BundledRulesError::ResponseBody {
                            group: label.clone(),
                            source,
                        })?
            {
                let room = max - body.len();
                body.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            return Err(BundledRulesError::Rejected {
                group: label,
                status,
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }
        installed.push(label);
    }
    Ok(installed)
}
