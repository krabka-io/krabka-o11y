use super::{Deserialize, Serialize};

/// The identity of one part inside a [`DeploymentCut`](super::DeploymentCut).
///
/// `manifest_sha256` is the SHA-256 of the stored part manifest. A restore
/// refuses a part whose manifest bytes differ.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CutPart {
    pub name: String,
    pub manifest_sha256: String,
    pub object_count: usize,
    pub byte_count: u64,
}
