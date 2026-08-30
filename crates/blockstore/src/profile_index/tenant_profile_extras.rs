use super::*;

#[derive(Default, Serialize, Deserialize)]
pub(crate) struct TenantProfileExtras {
    pub(crate) profile_types: BTreeMap<String, BTreeSet<SeriesFingerprint>>,
}
