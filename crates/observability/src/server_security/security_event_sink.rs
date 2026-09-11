use std::{fmt, sync::Arc};

use super::{NoSecurityEvents, SecurityEvents};

/// A shared handle to the [`SecurityEvents`] of one [`ServerSecurity`](super::ServerSecurity).
///
/// An authenticated [`Principal`](super::Principal) carries one, so the
/// authorization helpers can report a denial without another argument.
#[derive(Clone)]
pub struct SecurityEventSink(Arc<dyn SecurityEvents>);

impl SecurityEventSink {
    /// A handle to `events`.
    #[must_use]
    pub fn new(events: Arc<dyn SecurityEvents>) -> Self {
        Self(events)
    }

    /// The events this handle reports to.
    #[must_use]
    pub fn events(&self) -> &dyn SecurityEvents {
        self.0.as_ref()
    }
}

impl Default for SecurityEventSink {
    fn default() -> Self {
        Self(Arc::new(NoSecurityEvents))
    }
}

impl fmt::Debug for SecurityEventSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecurityEventSink(..)")
    }
}

impl PartialEq for SecurityEventSink {
    // Two handles are equal when they report to the same implementation.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for SecurityEventSink {}
