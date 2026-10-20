//! An event log of security events, and tenant ids, shared by the
//! `server_security` unit tests and the `server_security` suite.
//!
//! Both reach this file with `#[path]`, so it names only external crates.

use std::sync::Mutex;

use krabka_blockstore::TenantId;

/// The tenant `id`, which the test asserts is valid.
pub fn tenant(id: &str) -> TenantId {
    TenantId::new(id).expect("a valid tenant id")
}

/// Every event, as text, so a test can compare whole sequences and search
/// them for credential bytes.
#[derive(Default)]
pub struct RecordedEvents(Mutex<Vec<String>>);

impl RecordedEvents {
    pub fn take(&self) -> Vec<String> {
        std::mem::take(
            &mut *self
                .0
                .lock()
                .expect("no test panics while holding the lock"),
        )
    }

    pub fn push(&self, event: String) {
        self.0
            .lock()
            .expect("no test panics while holding the lock")
            .push(event);
    }

    /// Records that `principal` was refused `tenant`.
    pub fn push_tenant_denied(&self, principal: &str, tenant: &TenantId) {
        self.push(format!("tenant denied {principal} {tenant}"));
    }

    /// Records that `principal` was refused an admin route.
    pub fn push_admin_denied(&self, principal: &str) {
        self.push(format!("admin denied {principal}"));
    }
}
