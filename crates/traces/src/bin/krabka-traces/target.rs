use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub(crate) enum Target {
    Distributor,
    BlockBuilder,
    LiveStore,
    Querier,
    QueryFrontend,
    Compactor,
    MetricsGenerator,
}
