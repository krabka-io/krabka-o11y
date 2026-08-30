use super::*;

/// Wall-clock source in epoch nanoseconds.
pub trait Clock: Send + Sync {
    fn now_ns(&self) -> i64;
}
