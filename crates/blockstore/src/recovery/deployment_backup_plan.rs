use super::{DeploymentPart, DrainedGroup};

/// What one deployment backup copies and what it requires of the broker.
#[derive(Clone, Debug)]
pub struct DeploymentBackupPlan {
    pub cut_id: String,
    /// The operator name of the broker-side snapshot. The cut records it.
    pub broker_capture: String,
    pub drained_groups: Vec<DrainedGroup>,
    pub parts: Vec<DeploymentPart>,
    /// The parts that the operator declares this deployment does not have.
    /// The cut records them, so an audit and a restore show the omission.
    pub omitted_parts: Vec<String>,
}
