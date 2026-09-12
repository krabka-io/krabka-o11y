use krabka_units::{
    convert::{ByteSizeExt as _, TimeExt as _},
    mebibytes, secs,
};

use super::{
    ArtifactCache, Mutex, NativeResolver, NativeSymbol, ObjectSymbolResolver, SymbolizeRequest,
    lock_recover,
};

pub struct FileSystemResolver {
    pub(crate) cache: Mutex<ArtifactCache>,
}

impl Default for FileSystemResolver {
    fn default() -> Self {
        Self {
            cache: Mutex::new(ArtifactCache::new(
                mebibytes(1024).bytes_usize(),
                secs(60).to_std(),
            )),
        }
    }
}

impl NativeResolver for FileSystemResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        let cached = lock_recover(&self.cache).get(&request.filename);
        let resolver = cached.unwrap_or_else(|| {
            let resolver = ObjectSymbolResolver::from_file(&request.filename).ok();
            lock_recover(&self.cache).insert(request.filename.clone(), resolver.clone());
            resolver
        });
        resolver
            .as_ref()
            .and_then(|resolver| resolver.symbolize(request))
    }
}
