use super::*;

pub(crate) type CompactionFrontierRefreshSource = (Arc<dyn ObjectStore>, ObjectPath);
