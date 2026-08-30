//! Symbolizer role plumbing.

use std::sync::Arc;

use krabka_pprof::{
    ChainedResolver, DebuginfodConfig, DebuginfodResolver, FileSystemResolver, NativeResolver,
    NativeSymbol, SymbolizeRequest,
};

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_pprof::DebuginfodConfig;
    use krabka_units::{mebibytes, millis, secs};

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
}

mod address_fallback_resolver;
mod build_label;
mod native_resolver_from_debuginfod_config;
mod native_resolver_from_debuginfod_urls;
mod run;
mod run_with_config;

pub use address_fallback_resolver::AddressFallbackResolver;
use build_label::build_label;
pub use native_resolver_from_debuginfod_config::native_resolver_from_debuginfod_config;
pub use native_resolver_from_debuginfod_urls::native_resolver_from_debuginfod_urls;
pub use run::run;
pub use run_with_config::run_with_config;
