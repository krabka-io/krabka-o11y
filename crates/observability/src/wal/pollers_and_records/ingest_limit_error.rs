use super::Error;

#[derive(Debug, Error)]
pub enum IngestLimitError {
    #[error("ingest unauthorized for tenant `{tenant}`: {reason}")]
    Unauthorized { tenant: String, reason: String },
    #[error("ingest quota exceeded for tenant `{tenant}`: {reason}")]
    RateLimited { tenant: String, reason: String },
    #[error("ingest quota check unavailable for tenant `{tenant}`: {reason}")]
    Unavailable { tenant: String, reason: String },
}
