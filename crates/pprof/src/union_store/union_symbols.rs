use super::{Arc, BTreeMap, Frame, ResolvedLocation, SymbolSource};

#[derive(Default)]
pub(crate) struct UnionSymbols {
    pub(crate) sources: BTreeMap<u64, Arc<dyn SymbolSource>>,
}

impl UnionSymbols {
    pub(crate) fn insert(&mut self, partition_base: u64, source: Arc<dyn SymbolSource>) {
        self.sources.insert(partition_base, source);
    }
}

impl SymbolSource for UnionSymbols {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> {
        let partition_base = partition & 0xff00_0000_0000_0000;
        self.sources
            .get(&partition_base)
            .map_or_else(Vec::new, |source| {
                source.resolve(partition ^ partition_base, id)
            })
    }

    fn resolve_locations(&self, partition: u64, id: u32) -> Vec<ResolvedLocation> {
        let partition_base = partition & 0xff00_0000_0000_0000;
        self.sources
            .get(&partition_base)
            .map_or_else(Vec::new, |source| {
                source.resolve_locations(partition ^ partition_base, id)
            })
    }
}
