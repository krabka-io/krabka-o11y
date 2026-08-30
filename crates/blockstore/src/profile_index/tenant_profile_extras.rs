use super::{BTreeMap, BTreeSet, Deserialize, Serialize, SeriesFingerprint};

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct TenantProfileExtras {
    pub(crate) profile_types: BTreeMap<String, BTreeSet<SeriesFingerprint>>,
}
