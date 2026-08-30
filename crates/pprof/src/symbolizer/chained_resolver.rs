use super::{Arc, NativeResolver, NativeSymbol, SymbolizeRequest};

#[derive(Default)]
pub struct ChainedResolver {
    pub(crate) resolvers: Vec<Arc<dyn NativeResolver>>,
}

impl ChainedResolver {
    #[must_use]
    pub fn new(resolvers: Vec<Arc<dyn NativeResolver>>) -> Self {
        Self { resolvers }
    }
}

impl NativeResolver for ChainedResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        self.resolvers
            .iter()
            .find_map(|resolver| resolver.symbolize(request))
    }
}
