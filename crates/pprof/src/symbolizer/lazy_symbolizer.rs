use super::{
    Arc, Frame, HashMap, Mutex, NativeResolver, RawLocation, ResolvedFunction, ResolvedLine,
    ResolvedLocation, ResolvedMapping, SymbolDb, SymbolSource, SymbolizeRequest, lock_recover,
};

pub struct LazySymbolizer<R: NativeResolver> {
    pub(crate) symbols: SymbolDb,
    pub(crate) resolver: Arc<R>,
    pub(crate) cache: Mutex<HashMap<SymbolizeRequest, Option<Vec<Frame>>>>,
}

impl<R: NativeResolver> LazySymbolizer<R> {
    #[must_use]
    pub fn new(symbols: SymbolDb, resolver: Arc<R>) -> Self {
        Self {
            symbols,
            resolver,
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn symbolize_location(&self, location: RawLocation) -> Vec<Frame> {
        if location.mapping.symbolization.has_functions() {
            return Vec::new();
        }
        let request = SymbolizeRequest {
            build_id: location.build_id,
            filename: location.filename,
            address: location
                .address
                .saturating_sub(location.mapping.memory_start)
                + location.mapping.file_offset,
        };
        if let Some(cached) = lock_recover(&self.cache).get(&request) {
            return cached.clone().unwrap_or_default();
        }
        let resolved = self.resolver.symbolize(&request).map(|symbols| {
            symbols
                .into_iter()
                .map(|symbol| Frame {
                    function: symbol.function,
                    file: symbol.file,
                    line: symbol.line,
                })
                .collect::<Vec<_>>()
        });
        lock_recover(&self.cache).insert(request, resolved.clone());
        resolved.unwrap_or_default()
    }

    fn symbolize_resolved_location(&self, address: u64, mapping: &ResolvedMapping) -> Vec<Frame> {
        if mapping.has_functions {
            return Vec::new();
        }
        let request = SymbolizeRequest {
            build_id: mapping.build_id.clone(),
            filename: mapping.filename.clone(),
            address: address.saturating_sub(mapping.memory_start) + mapping.file_offset,
        };
        if let Some(cached) = lock_recover(&self.cache).get(&request) {
            return cached.clone().unwrap_or_default();
        }
        let resolved = self.resolver.symbolize(&request).map(|symbols| {
            symbols
                .into_iter()
                .map(|symbol| Frame {
                    function: symbol.function,
                    file: symbol.file,
                    line: symbol.line,
                })
                .collect::<Vec<_>>()
        });
        lock_recover(&self.cache).insert(request, resolved.clone());
        resolved.unwrap_or_default()
    }
}

impl<R: NativeResolver> SymbolSource for LazySymbolizer<R> {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> {
        let frames = self.symbols.resolve(partition, id);
        if !frames.is_empty() {
            return frames;
        }
        self.symbols
            .raw_locations(partition, id)
            .into_iter()
            .flat_map(|location| self.symbolize_location(location))
            .collect()
    }

    fn resolve_locations(&self, partition: u64, id: u32) -> Vec<ResolvedLocation> {
        let mut locations = self.symbols.resolve_locations(partition, id);
        for location in &mut locations {
            if !location.lines.is_empty() {
                continue;
            }
            let Some(mapping) = location.mapping.as_ref() else {
                continue;
            };
            location.lines = self
                .symbolize_resolved_location(location.address, mapping)
                .into_iter()
                .map(|frame| ResolvedLine {
                    function: ResolvedFunction {
                        name: frame.function.clone(),
                        system_name: frame.function,
                        filename: frame.file,
                        start_line: 0,
                    },
                    line: i64::from(frame.line),
                })
                .collect();
        }
        locations
    }
}
