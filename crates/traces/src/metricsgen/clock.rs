//! Injectable clock for deterministic metrics-generator tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn mock_clock_advances() {
        let c = MockClock::new(1_000);
        assert2::assert!(c.now_ns() == 1_000);
        c.advance(500);
        assert2::assert!(c.now_ns() == 1_500);
        c.set(42);
        assert2::assert!(c.now_ns() == 42);
    }
}

mod clock_type;
mod mock_clock;
mod system_clock;

pub use clock_type::Clock;
pub use mock_clock::MockClock;
pub use system_clock::SystemClock;
