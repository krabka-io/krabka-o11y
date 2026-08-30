use super::*;

#[derive(Debug, Error)]
pub enum QueryAuthorizationError {
    #[error("query unauthorized for tenant `{tenant}`: {reason}")]
    Unauthorized { tenant: String, reason: String },
    #[error("query authorization check unavailable for tenant `{tenant}`: {reason}")]
    Unavailable { tenant: String, reason: String },
}
