use super::{Arc, ObjectPath, ObjectStore};

pub(crate) type CompactionFrontierRefreshSource = (Arc<dyn ObjectStore>, ObjectPath);
