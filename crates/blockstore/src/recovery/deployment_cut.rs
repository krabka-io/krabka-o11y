use super::{BrokerSnapshot, CutPart, Deserialize, Serialize};

/// The completion record of a deployment backup set.
///
/// The record binds every part manifest, by digest, to one broker snapshot.
/// A backup writes it last, so a set without it is incomplete.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeploymentCut {
    pub schema_version: u32,
    pub cut_id: String,
    /// The operator name of the broker-side snapshot, for example the
    /// `krabka-backup` capture id.
    pub broker_capture: String,
    pub broker: BrokerSnapshot,
    pub parts: Vec<CutPart>,
    /// The parts that the operator declared the deployment does not have,
    /// sorted. No part of the cut has one of these names.
    #[serde(default)]
    pub omitted_parts: Vec<String>,
}
