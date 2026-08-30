use super::{NativeSymbol, SymbolizeRequest};

pub trait NativeResolver: Send + Sync {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>>;
}
