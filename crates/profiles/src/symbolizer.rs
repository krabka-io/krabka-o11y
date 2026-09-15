//! Symbolizer role plumbing.

use std::sync::Arc;

use krabka_blockstore::ProfileIndex;
use krabka_pprof::{
    ChainedResolver, DebuginfodConfig, DebuginfodResolver, FileSystemResolver, NativeResolver,
    NativeSymbol, SymbolizeRequest,
};
use object_store::{ObjectStore, path::Path};

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_blockstore::{BlockIndex, BlockLevel, BlockMeta, ProfileIndex};
    use krabka_pprof::DebuginfodConfig;
    use krabka_units::{mebibytes, millis, secs};
    use object_store::{ObjectStoreExt as _, memory::InMemory, path::Path};

    use super::*;

    #[test]
    fn fallback_resolver_names_build_id_and_offset() {
        let out = AddressFallbackResolver
            .symbolize(&SymbolizeRequest {
                build_id: "abc".to_string(),
                filename: "/bin/app".to_string(),
                address: 0x42,
            })
            .unwrap();

        assert!(out[0].function == "abc+0x42");
        assert!(out[0].file == "/bin/app");
    }

    #[test]
    fn symbolizer_builds_local_plus_debuginfod_resolver() {
        native_resolver_from_debuginfod_urls(vec!["http://127.0.0.1:1".to_string()]).unwrap();
    }

    #[test]
    fn symbolizer_accepts_explicit_debuginfod_config() {
        let config = DebuginfodConfig::new(mebibytes(64), millis(250), secs(3)).unwrap();

        native_resolver_from_debuginfod_config(vec!["http://127.0.0.1:1".to_string()], config)
            .unwrap();
    }

    #[test]
    fn native_resolver_falls_back_to_address_frame() {
        let resolver = native_resolver_from_debuginfod_urls(Vec::new()).unwrap();
        let out = resolver
            .symbolize(&SymbolizeRequest {
                build_id: String::new(),
                filename: "/missing/native".to_string(),
                address: 0x99,
            })
            .unwrap();

        assert!(out[0].function == "/missing/native+0x99");
        assert!(out[0].file == "/missing/native");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn offline_pass_reads_the_uploaded_elf_from_the_blocks_tenant() {
        use krabka_pprof::{LocationRec, MappingRec, MappingSymbolization, SymbolDb};

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut symbols = SymbolDb::new();
        let filename = symbols.intern_string("/missing/native");
        let build_id = symbols.intern_string("deadbeef");
        let mapping = symbols.intern_mapping(MappingRec {
            memory_start: 0,
            memory_limit: u64::MAX,
            file_offset: 0,
            filename,
            build_id,
            symbolization: MappingSymbolization::default(),
        });
        let location = symbols.intern_location(LocationRec {
            address: 0,
            mapping_id: mapping,
            lines: Vec::new(),
        });
        let stack = symbols.intern_stacktrace(0, &[location]);
        let object_key = "blocks/tenant-a/test.parquet";
        let other_object_key = "blocks/tenant-a/other.parquet";
        for key in [object_key, other_object_key] {
            store
                .put(&Path::from(format!("{key}.symdb")), symbols.encode().into())
                .await
                .unwrap();
        }
        store
            .put(
                &Path::from("debug-info/tenant-a/deadbeef/exe"),
                std::fs::read(std::env::current_exe().unwrap())
                    .unwrap()
                    .into(),
            )
            .await
            .unwrap();
        let mut index = ProfileIndex::new();
        BlockIndex::add_block(
            &mut index,
            &BlockMeta {
                tenant: "tenant-a".to_string(),
                object_key: object_key.to_string(),
                min_ts: 0,
                max_ts: 1,
                row_count: 1,
                fingerprints: Vec::new(),
                level: BlockLevel::INGESTED,
            },
        );
        index.add_profile_block("tenant-a", object_key, vec![0]);
        BlockIndex::add_block(
            &mut index,
            &BlockMeta {
                tenant: "tenant-a".to_string(),
                object_key: other_object_key.to_string(),
                min_ts: 2,
                max_ts: 3,
                row_count: 1,
                fingerprints: Vec::new(),
                level: BlockLevel::INGESTED,
            },
        );
        index.add_profile_block("tenant-a", other_object_key, vec![0]);

        let metrics = crate::metrics::ServiceMetrics::new();
        assert!(
            symbolize_blocks_once(&store, &index, &AddressFallbackResolver, &metrics,)
                .await
                .unwrap()
                == 2
        );
        for (status, expected) in [("miss", 1), ("hit", 1)] {
            assert!(
                metrics
                    .symbolizer_cache_requests
                    .get_or_create(&crate::metrics::StatusLabel {
                        status: status.to_string(),
                    })
                    .get()
                    == expected
            );
        }
        let updated = store
            .get(&Path::from(format!("{object_key}.symdb")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let updated = SymbolDb::decode(&updated).unwrap();
        let frames = updated.resolve(0, stack);
        assert!(!frames.is_empty());
        assert!(frames[0].function != "deadbeef+0x0", "{frames:?}");
    }
}

mod address_fallback_resolver;
mod build_label;
mod native_resolver_from_debuginfod_config;
mod native_resolver_from_debuginfod_urls;
mod offline_resolver_from_debuginfod_config;
mod run;
mod run_with_config;
mod symbolize_blocks_once;

pub use address_fallback_resolver::AddressFallbackResolver;
use build_label::build_label;
pub use native_resolver_from_debuginfod_config::native_resolver_from_debuginfod_config;
pub use native_resolver_from_debuginfod_urls::native_resolver_from_debuginfod_urls;
pub use offline_resolver_from_debuginfod_config::offline_resolver_from_debuginfod_config;
pub use run::run;
pub use run_with_config::run_with_config;
pub use symbolize_blocks_once::symbolize_blocks_once;
