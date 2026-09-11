use super::ValueEnum;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub(crate) enum Target {
    Distributor,
    Compactor,
    Querier,
    QueryFrontend,
    Ruler,
}

impl Target {
    /// Whether this role reaches the broker, and so cannot run correctly
    /// unless the topic contract holds.
    ///
    /// Three of these roles serve HTTP and never open a client: asking them to
    /// validate a topic would make a broker they do not use a condition of
    /// their starting, which is a fault they do not have. The match is
    /// exhaustive so a new role has to answer the question rather than
    /// inherit an answer.
    pub(crate) fn touches_the_wal(self) -> bool {
        match self {
            Self::Distributor | Self::Compactor => true,
            Self::Querier | Self::QueryFrontend | Self::Ruler => false,
        }
    }
}
