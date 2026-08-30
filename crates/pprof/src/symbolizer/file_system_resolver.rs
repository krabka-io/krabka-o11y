use super::{
    HashMap, Mutex, NativeResolver, NativeSymbol, ObjectSymbolResolver, SymbolizeRequest,
    lock_recover,
};

#[derive(Default)]
pub struct FileSystemResolver {
    pub(crate) cache: Mutex<HashMap<String, Option<ObjectSymbolResolver>>>,
}

impl NativeResolver for FileSystemResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        let mut cache = lock_recover(&self.cache);
        let resolver = cache
            .entry(request.filename.clone())
            .or_insert_with(|| ObjectSymbolResolver::from_file(&request.filename).ok());
        resolver
            .as_ref()
            .and_then(|resolver| resolver.symbolize(request))
    }
}
