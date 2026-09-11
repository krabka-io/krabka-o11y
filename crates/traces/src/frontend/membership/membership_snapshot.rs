use super::QuerierMember;

/// One consistent view of the querier pool, taken at an instant.
///
/// A query plans, fans out and merges against a single snapshot. Planning
/// against one membership and collecting against another is how a fan-out
/// loses a shard without noticing: the job went to a querier that the
/// collecting view no longer contains, so nothing is left to attribute the
/// gap to. The `generation` makes a change across a query detectable, and
/// [`QueryFrontend::search`](crate::frontend::QueryFrontend::search) reports
/// one that happened while a live shard was in flight.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Membership {
    members: Vec<QuerierMember>,
    generation: u64,
}

impl Membership {
    /// A snapshot of `members`, ordered by address so assignment is
    /// reproducible whatever order DNS and the probes answered in.
    #[must_use]
    pub fn new(mut members: Vec<QuerierMember>, generation: u64) -> Self {
        members.sort_by(|a, b| a.addr.cmp(&b.addr));
        members.dedup_by(|a, b| a.addr == b.addr);
        Self {
            members,
            generation,
        }
    }

    /// Which refresh produced this snapshot. It only ever increases.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Every known querier, ready or not.
    #[must_use]
    pub fn members(&self) -> &[QuerierMember] {
        &self.members
    }

    /// The addresses a job may be assigned to, in address order.
    #[must_use]
    pub fn ready_addrs(&self) -> Vec<&str> {
        self.members
            .iter()
            .filter(|m| m.is_ready())
            .map(|m| m.addr.as_str())
            .collect()
    }

    /// How many queriers may take a job.
    #[must_use]
    pub fn ready_count(&self) -> usize {
        self.members.iter().filter(|m| m.is_ready()).count()
    }

    /// One warning line per querier left out of the fan-out.
    #[must_use]
    pub fn exclusion_warnings(&self) -> Vec<String> {
        self.members
            .iter()
            .filter_map(QuerierMember::warning)
            .collect()
    }
}
