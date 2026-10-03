use super::{Arc, ObjectStore};

/// One named store of a deployment: a signal bucket or a local state
/// directory.
///
/// A backup reads the part from `store`. A restore writes the part into
/// `store`, which must be empty or hold an earlier pass of the same restore.
#[derive(Clone, Debug)]
pub struct DeploymentPart {
    pub name: String,
    pub store: Arc<dyn ObjectStore>,
}
