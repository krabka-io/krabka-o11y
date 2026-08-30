use super::*;

/// Not `Eq`: the sharded variant carries a [`FrontendConfig`], whose shard width
/// is a `f64`-backed quantity.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum QueryExecution {
    Direct,
    Sharded(FrontendConfig),
}
