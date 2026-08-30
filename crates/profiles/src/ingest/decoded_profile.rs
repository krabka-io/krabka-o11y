use super::*;

/// One series after the multi-value split: a single `__profile_type__`.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedProfile {
    pub labels: Labels,
    pub profile_type: String,
    pub samples: Vec<DecodedSample>,
}
