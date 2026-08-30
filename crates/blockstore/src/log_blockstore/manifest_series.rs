use super::*;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ManifestSeries {
    pub(crate) tenant: String,
    pub(crate) fingerprint: SeriesFingerprint,
    pub(crate) labels: Labels,
}
