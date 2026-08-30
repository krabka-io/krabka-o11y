use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Role {
    Distributor,
    Compactor,
    Querier,
}
