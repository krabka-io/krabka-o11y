use super::Url;

/// Whether a store has to honour a conditional update for Krabka to run on it.
///
/// Krabka writes create-if-absent on every backend. It writes a conditional
/// update, which is `If-Match` on an `ETag` or a GCS generation, to reclaim
/// superseded index payloads and to swap profiles recording rules. A local
/// filesystem cannot do that write, and a single process that owns its
/// directory does not need it. A shared bucket does: without it, two writers
/// can each replace the other's object and neither is told.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConditionalUpdateRequirement {
    /// The probe fails when the store does not implement a conditional update.
    Required,
    /// The probe reports a missing conditional update and does not fail.
    Optional,
}

impl ConditionalUpdateRequirement {
    /// The requirement for the store that `url` configures.
    ///
    /// `file://`, `memory://` and a bare relative path are local to one
    /// process, so a conditional update is optional there. Every other scheme
    /// names a store that more than one process can reach, so it is required.
    #[must_use]
    pub fn for_object_store_url(url: &str) -> Self {
        match Url::parse(url) {
            Ok(parsed) if matches!(parsed.scheme(), "file" | "memory") => Self::Optional,
            Err(url::ParseError::RelativeUrlWithoutBase) => Self::Optional,
            _ => Self::Required,
        }
    }
}
