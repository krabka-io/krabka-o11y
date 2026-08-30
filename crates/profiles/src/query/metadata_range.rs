use super::{ProfileError, ProfileStore, QuerierState};

#[derive(Clone, Copy)]
pub(crate) struct MetadataRange {
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
    pub(crate) omitted: bool,
}

impl MetadataRange {
    pub(crate) fn from_request(start_ms: i64, end_ms: i64) -> Self {
        let omitted = start_ms == 0 && end_ms == 0;
        if omitted {
            Self {
                start_ms: 0,
                end_ms: i64::MAX,
                omitted,
            }
        } else {
            Self {
                start_ms,
                end_ms,
                omitted,
            }
        }
    }

    pub(crate) fn validate<S: ProfileStore>(
        self,
        state: &QuerierState<S>,
        tenant: &str,
    ) -> Result<Self, ProfileError> {
        if !self.omitted {
            state.validate_query_range(tenant, self.start_ms, self.end_ms)?;
        }
        Ok(self)
    }
}
