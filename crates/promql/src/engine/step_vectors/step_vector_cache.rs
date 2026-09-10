use super::{Arc, Mutex, StepVectorCacheInner};

/// The range driver's grid-driven leaf memo, shared across its step loop.
pub(crate) type StepVectorCache = Arc<Mutex<StepVectorCacheInner>>;
