/// One stage of a signal's pipeline, named as the upstream this stack tracks
/// names it.
///
/// The names come from Loki, Mimir, Tempo and Pyroscope, because an operator
/// arriving from any of them should not have to learn a second word for a
/// stage they already run. Where those four disagreed, the majority settled
/// it; where a stage exists in only one of them, that one settled it.
///
/// Not every signal has every stage, and the gaps are real rather than
/// oversights. Logs and metrics have no [`LiveStore`](Self::LiveStore) because
/// their queriers read the recent window from the WAL themselves; only traces
/// keeps that tier as a role. [`Ruler`](Self::Ruler) is a metrics stage
/// because only metrics evaluates recording and alerting rules, and
/// [`Symbolizer`](Self::Symbolizer) is a profiles stage because only profiles
/// resolves addresses to function names.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RoleKind {
    /// Accepts pushes, applies limits, and writes the signal's WAL topic.
    ///
    /// All four upstreams call it this.
    Distributor,
    /// Consumes the WAL topic and writes blocks to object storage.
    ///
    /// Mimir and Tempo both spell this stage `block-builder` on the Kafka
    /// ingest path that Krabka is built on, and Loki spelled it that way too
    /// before renaming it for its own object format. Metrics and logs called
    /// it `compactor` here, which put one word on two different jobs -- this
    /// stage in two signals, and [`Compactor`](Self::Compactor) in the other
    /// two. `block-builder` is the half with upstream behind it.
    BlockBuilder,
    /// Holds the window of data that no block covers yet, and answers queries
    /// over it.
    ///
    /// Tempo's `live-store`, which "serves recent trace data, holding traces
    /// in memory ... so they're queryable within seconds of ingestion". It is
    /// the only upstream with the stage and so the only one that could name
    /// it.
    LiveStore,
    /// Answers a query over the blocks it is given, and over whatever live
    /// tier it can reach.
    ///
    /// All four upstreams call it this.
    Querier,
    /// Splits a query into jobs, fans them out to queriers, and merges what
    /// comes back.
    ///
    /// All four upstreams call it this.
    QueryFrontend,
    /// Merges blocks that are already in object storage into larger ones, and
    /// enforces retention.
    ///
    /// Mimir's `compactor` is exactly this. Loki's is the same job over its
    /// index, and carries the delete requests too. A compactor reads and
    /// writes object storage only: it never touches the WAL, which is what
    /// separates it from [`BlockBuilder`](Self::BlockBuilder).
    Compactor,
    /// Evaluates recording and alerting rules. Loki's and Mimir's `ruler`.
    Ruler,
    /// Derives metrics from spans and remote-writes them. Tempo's
    /// `metrics-generator`.
    MetricsGenerator,
    /// Resolves profile addresses to function names. Pyroscope's `symbolizer`.
    Symbolizer,
    /// Every role of the signal, in one process.
    ///
    /// Loki, Mimir, Tempo and Pyroscope all spell the single-process mode
    /// `all`, so this does too.
    All,
}

impl RoleKind {
    /// The name an operator types and a manifest carries.
    ///
    /// This is the same string the binaries' `--target` accepts, and
    /// deliberately so: one vocabulary means one spelling, and each signal's
    /// own `--target` enum is checked against these names.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Distributor => "distributor",
            Self::BlockBuilder => "block-builder",
            Self::LiveStore => "live-store",
            Self::Querier => "querier",
            Self::QueryFrontend => "query-frontend",
            Self::Compactor => "compactor",
            Self::Ruler => "ruler",
            Self::MetricsGenerator => "metrics-generator",
            Self::Symbolizer => "symbolizer",
            Self::All => "all",
        }
    }

    /// Whether this names a composition of the other stages rather than a
    /// stage of its own.
    #[must_use]
    pub const fn is_composite(self) -> bool {
        matches!(self, Self::All)
    }
}

impl std::fmt::Display for RoleKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
