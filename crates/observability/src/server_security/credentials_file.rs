use serde::Deserialize;

use super::principal_entry::PrincipalEntry;

/// The credentials file as YAML gives it, before validation.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialsFile {
    pub principals: Vec<PrincipalEntry>,
}
