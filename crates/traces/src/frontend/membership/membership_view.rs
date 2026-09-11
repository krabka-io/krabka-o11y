use super::{Arc, ArcSwap, Membership, QuerierMember};

/// The shared, swappable membership the refresh loop writes and every query
/// reads.
///
/// Cloning it shares the same cell. A reader takes an `Arc<Membership>` and
/// holds it for the whole query, so a refresh mid-query replaces the cell
/// without moving the ground under a fan-out that is already in flight.
#[derive(Clone)]
pub struct MembershipView {
    inner: Arc<ArcSwap<Membership>>,
}

impl MembershipView {
    /// A view that knows no queriers yet, as a frontend does before its first
    /// probe. Every query against it fails rather than answering from nothing.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(Membership::default())),
        }
    }

    /// A view whose members never change and are all ready.
    ///
    /// This is the in-process case: a test, or any caller that has already
    /// decided who the queriers are.
    #[must_use]
    pub fn fixed<I, S>(addrs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let members = addrs.into_iter().map(QuerierMember::ready).collect();
        Self {
            inner: Arc::new(ArcSwap::from_pointee(Membership::new(members, 0))),
        }
    }

    /// The current snapshot. Hold the returned handle for a whole query.
    #[must_use]
    pub fn load(&self) -> Arc<Membership> {
        self.inner.load_full()
    }

    /// Replaces the snapshot with `members`, at the next generation.
    pub fn publish(&self, members: Vec<QuerierMember>) {
        let generation = self.inner.load().generation().wrapping_add(1);
        self.inner
            .store(Arc::new(Membership::new(members, generation)));
    }
}

impl Default for MembershipView {
    fn default() -> Self {
        Self::empty()
    }
}
