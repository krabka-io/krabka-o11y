use super::{UnixNanos, i64};

impl From<UnixNanos> for i64 {
    fn from(instant: UnixNanos) -> Self {
        instant.0
    }
}
