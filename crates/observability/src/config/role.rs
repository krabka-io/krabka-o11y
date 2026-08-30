use super::ValueEnum;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Role {
    Distributor,
    Compactor,
    Querier,
}
