use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub(crate) enum Target {
    Distributor,
    BlockBuilder,
    Querier,
    QueryFrontend,
    Compactor,
    Symbolizer,
}
