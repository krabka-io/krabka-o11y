use serde::Deserialize;

/// One entry of `principals`, before validation.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalEntry {
    pub name: String,
    #[serde(default)]
    pub token_sha256: Vec<String>,
    #[serde(default)]
    pub client_certificates: Vec<String>,
    pub tenants: Vec<String>,
    #[serde(default)]
    pub admin: bool,
}
