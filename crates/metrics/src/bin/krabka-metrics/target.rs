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
