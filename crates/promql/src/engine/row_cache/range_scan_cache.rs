use super::{Arc, Mutex, RangeScanCacheInner};

pub(crate) type RangeScanCache = Arc<Mutex<RangeScanCacheInner>>;
