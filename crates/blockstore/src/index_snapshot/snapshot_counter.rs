use super::AtomicU64;

pub(crate) static SNAPSHOT_COUNTER: AtomicU64 = AtomicU64::new(0);
