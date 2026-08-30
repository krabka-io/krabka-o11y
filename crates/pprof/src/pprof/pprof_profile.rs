use super::{Message, ProfileError};

/// A decoded perftools.profiles profile.
#[derive(Clone, Debug, PartialEq)]
pub struct PprofProfile {
    pub(crate) inner: crate::proto::Profile,
}

impl PprofProfile {
    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    pub fn decode(bytes: &[u8]) -> Result<Self, ProfileError> {
        crate::proto::Profile::decode(bytes)
            .map(|inner| Self { inner })
            .map_err(ProfileError::from)
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        self.inner.encode_to_vec()
    }

    #[must_use]
    pub fn inner(&self) -> &crate::proto::Profile {
        &self.inner
    }

    #[must_use]
    pub fn into_inner(self) -> crate::proto::Profile {
        self.inner
    }

    #[must_use]
    pub fn string(&self, index: i64) -> Option<&str> {
        usize::try_from(index)
            .ok()
            .and_then(|idx| self.inner.string_table.get(idx))
            .map(String::as_str)
    }

    #[must_use]
    pub fn sample_types(&self) -> Vec<(String, String)> {
        self.inner
            .sample_type
            .iter()
            .map(|sample_type| {
                (
                    self.string(sample_type.r#type).unwrap_or("").to_string(),
                    self.string(sample_type.unit).unwrap_or("").to_string(),
                )
            })
            .collect()
    }

    #[must_use]
    pub fn period_type_strings(&self) -> (String, String) {
        self.inner.period_type.map_or_else(
            || (String::new(), String::new()),
            |period_type| {
                (
                    self.string(period_type.r#type).unwrap_or("").to_string(),
                    self.string(period_type.unit).unwrap_or("").to_string(),
                )
            },
        )
    }

    /// Frame names for `sample`, leaf first, in the order pprof stores
    /// `location_id`.
    ///
    /// A location resolves through its first line to a function, and the
    /// function's name through the string table. Ids that resolve to none of
    /// those are skipped: a profile that names only some of its frames is
    /// still worth reading.
    #[must_use]
    pub fn stack_frames(&self, sample: &crate::proto::Sample) -> Vec<&str> {
        sample
            .location_id
            .iter()
            .filter_map(|id| {
                let location = self.inner.location.iter().find(|loc| loc.id == *id)?;
                let line = location.line.first()?;
                let function = self
                    .inner
                    .function
                    .iter()
                    .find(|func| func.id == line.function_id)?;
                self.string(function.name)
            })
            .collect()
    }

    #[must_use]
    pub fn samples(&self) -> &[crate::proto::Sample] {
        &self.inner.sample
    }
}

impl From<crate::proto::Profile> for PprofProfile {
    fn from(inner: crate::proto::Profile) -> Self {
        Self { inner }
    }
}
