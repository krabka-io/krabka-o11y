use super::*;

pub(crate) fn local_native_resolver() -> Arc<ChainedResolver> {
    Arc::new(ChainedResolver::new(vec![
        Arc::new(FileSystemResolver::default()),
        Arc::new(AddressFallbackResolver),
    ]))
}
