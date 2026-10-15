//! Loading bundled rule files and installing their groups through the ruler API.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use krabka_observability::server_security::ServerSecurity;

use super::{
    ByteSize, ByteSizeExt, PathBuf, StatusCode, StdPath, TENANT_HEADER, TenantId, Url, header,
    kibibytes,
};

/// Names one group of a bundled rule file for an error message.
///
/// The ruler config API rejects a group that carries no name. The position in
/// the file names that group instead, so an operator can find it.
pub(crate) fn bundled_group_label(index: usize, group: &serde_yaml::Value) -> String {
    group
        .get("name")
        .and_then(serde_yaml::Value::as_str)
        .filter(|name| !name.is_empty())
        .map_or_else(|| format!("#{index}"), str::to_string)
}

/// One Prometheus rule file, as the ruler reads it from disk.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct BundledRuleFile {
    pub(crate) groups: Vec<serde_yaml::Value>,
}

/// The address that the ruler dials to post its bundled rule groups to its own `listener`.
///
/// A listener on every interface, `0.0.0.0` or `::`, gets the loopback address
/// of the same family. A listener on one address gets that address.
#[must_use]
pub fn bundled_rules_address(listener: SocketAddr) -> SocketAddr {
    let ip = match listener.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, listener.port())
}

/// Errors that stop the ruler from installing a bundled rule file.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BundledRulesError {
    #[error("bundled rule file `{path}` is unreadable: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("bundled rule file `{path}` is not a Prometheus rule file: {source}")]
    Decode {
        path: PathBuf,
        #[source]
        source: serde_yaml::Error,
    },

    #[error("bundled rule file `{path}` holds no rule group")]
    NoGroups { path: PathBuf },

    #[error("bundled rule file `{path}` has no file stem to name the rule namespace")]
    NoNamespace { path: PathBuf },

    #[error("bundled rule group `{group}` does not encode back to YAML: {source}")]
    Encode {
        group: String,
        #[source]
        source: serde_yaml::Error,
    },

    /// The internal client credentials do not make a usable HTTP client.
    #[error("the HTTP client for the ruler config API does not build: {source}")]
    Client {
        #[source]
        source: reqwest::Error,
    },

    /// The URL of the ruler's own listener does not parse.
    #[error("the ruler config URL `{address}` is not valid: {source}")]
    Url {
        address: String,
        #[source]
        source: url::ParseError,
    },

    /// The request did not reach the ruler's listener, or got no response.
    #[error("the ruler config request for bundled rule group `{group}` failed: {source}")]
    Send {
        group: String,
        #[source]
        source: reqwest::Error,
    },

    #[error("the ruler config API rejected bundled rule group `{group}`: HTTP {status}, {body}")]
    Rejected {
        group: String,
        status: StatusCode,
        body: String,
    },

    #[error("the ruler config response for bundled rule group `{group}` is unreadable: {source}")]
    ResponseBody {
        group: String,
        #[source]
        source: reqwest::Error,
    },
}

/// The host name that the ruler uses to post its bundled rule groups to its own listener.
///
/// The HTTP client resolves this name to the listener's address and does not
/// ask DNS. A TLS certificate should be valid for this name.
pub const BUNDLED_RULES_HOST: &str = "localhost";

/// Names the rule namespace of a bundled rule file from its file stem.
pub(crate) fn bundled_rules_namespace(path: &StdPath) -> Result<String, BundledRulesError> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(str::to_string)
        .ok_or_else(|| BundledRulesError::NoNamespace {
            path: path.to_path_buf(),
        })
}

/// The largest ruler-config response body this reads back for an error message.
pub(crate) const BUNDLED_RULES_RESPONSE_MAX: ByteSize = kibibytes(64);

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
    for entry in file.groups.iter().enumerate() {
        let index = entry.0;
        let group = entry.1;
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
