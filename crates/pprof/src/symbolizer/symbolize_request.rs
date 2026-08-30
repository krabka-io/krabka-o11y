#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SymbolizeRequest {
    pub build_id: String,
    pub filename: String,
    pub address: u64,
}
