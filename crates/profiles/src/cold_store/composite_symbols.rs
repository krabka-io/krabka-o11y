use super::{Arc, ExternalPartition, Frame, HashMap, LocalPartition, SymbolSource};

pub(crate) struct PartitionSymbols {
    symbols: Arc<dyn SymbolSource>,
    local_partition: LocalPartition,
}

#[derive(Default)]
pub(crate) struct CompositeSymbols {
    pub(crate) by_partition: HashMap<ExternalPartition, PartitionSymbols>,
}

impl CompositeSymbols {
    pub(crate) fn insert(
        &mut self,
        external_partition: ExternalPartition,
        symbols: Arc<dyn SymbolSource>,
        local_partition: LocalPartition,
    ) {
        self.by_partition.insert(
            external_partition,
            PartitionSymbols {
                symbols,
                local_partition,
            },
        );
    }
}

impl SymbolSource for CompositeSymbols {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> {
        self.by_partition
            .get(&ExternalPartition(partition))
            .map_or_else(Vec::new, |partition| {
                partition.symbols.resolve(partition.local_partition.0, id)
            })
    }
}
