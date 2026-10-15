//! Lazy native symbolization wrapper.

use std::{
    collections::HashMap,
    io::Read,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
};

use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    mebibytes, secs,
};
use object::{Object as _, ObjectSymbol as _};
use refined_type::{Refined, rule::GreaterU64};

use crate::{
    Frame, RawLocation, ResolvedFunction, ResolvedLine, ResolvedLocation, ResolvedMapping,
    SymbolDb, SymbolSource,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeSymbol {
    pub function: String,
    pub file: String,
    pub line: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SymbolizeRequest {
    pub build_id: String,
    pub filename: String,
    pub address: u64,
}

pub trait NativeResolver: Send + Sync {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>>;
}

pub(crate) fn nearest_symbol_name(object: &object::File<'_>, address: u64) -> Option<String> {
    object
        .symbols()
        .filter(|symbol| symbol.address() <= address)
        .filter(|symbol| {
            let size = symbol.size();
            size == 0 || address < symbol.address().saturating_add(size)
        })
        .max_by_key(object::ObjectSymbol::address)
        .and_then(|symbol| symbol.name().ok())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use assert2::{assert, check};
    use krabka_units::millis;
    // Only used by the ELF/DWARF self-symbolization tests below, which run on Linux.
    #[cfg(target_os = "linux")]
    use object::{Object, ObjectSymbol};

    use super::*;

    struct SymbolRecord {
        address: u64,
        size: u64,
        name: String,
    }

    struct ZeroSizeSymbols {
        address: u64,
        names: Vec<String>,
    }
    use crate::{LocationRec, MappingRec, MappingSymbolization};

    #[test]
    fn debuginfod_config_preserves_defaults_and_custom_values() {
        let defaults = DebuginfodConfig::default();
        assert!(defaults.max_artifact_size() == mebibytes(512));
        assert!(defaults.connect_timeout() == secs(5));
        assert!(defaults.request_timeout() == secs(10));

        let custom = DebuginfodConfig::new(mebibytes(64), millis(250), secs(3)).unwrap();
        assert!(custom.max_artifact_size() == mebibytes(64));
        assert!(custom.connect_timeout() == millis(250));
        assert!(custom.request_timeout() == secs(3));
    }

    #[test]
    fn debuginfod_config_rejects_invalid_values() {
        for result in [
            DebuginfodConfig::new(ByteSize::ZERO, secs(1), secs(2)),
            DebuginfodConfig::new(ByteSize::from_bytes_f64(0.5), secs(1), secs(2)),
            DebuginfodConfig::new(mebibytes(1), Time::ZERO, secs(2)),
            DebuginfodConfig::new(mebibytes(1), Time::from_secs_f64(f64::INFINITY), secs(2)),
            DebuginfodConfig::new(mebibytes(1), secs(3), secs(2)),
        ] {
            assert!(result.is_err());
        }
    }

    #[test]
    fn artifact_cache_evicts_by_bytes_and_expires_negative_entries() {
        let mut cache = ArtifactCache::new(3, std::time::Duration::from_mins(1));
        cache.insert("aa".into(), None);
        cache.insert("bb".into(), None);
        assert!(cache.get("aa").is_none());
        assert!(matches!(cache.get("bb"), Some(None)));

        let mut cache = ArtifactCache::new(10, std::time::Duration::ZERO);
        cache.insert("missing".into(), None);
        assert!(cache.get("missing").is_none());
    }

    struct FixedResolver {
        calls: AtomicUsize,
        expected_address: u64,
    }

    impl NativeResolver for FixedResolver {
        fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            assert!(request.build_id == "build-a");
            assert!(request.address == self.expected_address);
            Some(vec![NativeSymbol {
                function: "native_main".to_string(),
                file: "main.c".to_string(),
                line: 42,
            }])
        }
    }

    #[cfg(target_os = "linux")]
    fn is_llvm_cov_run() -> bool {
        std::env::var_os("LLVM_PROFILE_FILE").is_some()
    }

    /// A database holding one unsymbolized mapping of `/bin/app` (build
    /// `build-a`) at `0x1000..0x2000`, file offset `0x30`, and that mapping's id.
    struct UnsymbolizedAppFixture {
        db: SymbolDb,
        mapping_id: u32,
    }

    fn db_with_unsymbolized_app_mapping() -> UnsymbolizedAppFixture {
        let mut db = SymbolDb::new();
        let filename = db.intern_string("/bin/app");
        let build_id = db.intern_string("build-a");
        let mapping = db.intern_mapping(MappingRec {
            memory_start: 0x1000,
            memory_limit: 0x2000,
            file_offset: 0x30,
            filename,
            build_id,
            symbolization: MappingSymbolization::default(),
        });
        UnsymbolizedAppFixture {
            db,
            mapping_id: mapping,
        }
    }

    #[test]
    fn lazy_symbolizer_resolves_unsymbolized_location_once() {
        let UnsymbolizedAppFixture {
            mut db,
            mapping_id: mapping,
        } = db_with_unsymbolized_app_mapping();
        let loc = db.intern_location(LocationRec {
            address: 0x1010,
            mapping_id: mapping,
            lines: Vec::new(),
        });
        let stack = db.intern_stacktrace(0, &[loc]);
        let resolver = Arc::new(FixedResolver {
            calls: AtomicUsize::new(0),
            expected_address: 0x40,
        });
        let source = LazySymbolizer::new(db, Arc::clone(&resolver));

        let first = source.resolve(0, stack);
        let second = source.resolve(0, stack);

        check!(first == second);
        check!(first[0].function == "native_main");
        check!(resolver.calls.load(Ordering::Relaxed) == 1);
    }

    #[test]
    fn partial_mapping_symbolization_leaves_unresolved_locations_pending() {
        struct PartialResolver;
        impl NativeResolver for PartialResolver {
            fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
                (request.address == 0x40).then(|| {
                    vec![NativeSymbol {
                        function: "resolved".into(),
                        file: "main.c".into(),
                        line: 1,
                    }]
                })
            }
        }

        let UnsymbolizedAppFixture {
            mut db,
            mapping_id: mapping,
        } = db_with_unsymbolized_app_mapping();
        for address in [0x1010, 0x1020] {
            db.intern_location(LocationRec {
                address,
                mapping_id: mapping,
                lines: Vec::new(),
            });
        }

        assert!(db.symbolize_native(&PartialResolver) == 1);
        assert!(
            db.pending_native_symbols()
                .iter()
                .any(|request| request.address == 0x50)
        );
    }

    #[test]
    fn lazy_symbolizer_keeps_presymbolized_frames() {
        let mut db = SymbolDb::new();
        let name = db.intern_string("known");
        let function = db.intern_function(crate::FunctionRec {
            name,
            system_name: name,
            filename: 0,
            start_line: 0,
        });
        let loc = db.intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![crate::LineRec {
                function_id: function,
                line: 7,
            }],
        });
        let stack = db.intern_stacktrace(0, &[loc]);
        let resolver = Arc::new(FixedResolver {
            calls: AtomicUsize::new(0),
            expected_address: 0,
        });
        let source = LazySymbolizer::new(db, Arc::clone(&resolver));

        let frames = source.resolve(0, stack);

        assert!(frames[0].function == "known");
        assert!(resolver.calls.load(Ordering::Relaxed) == 0);
    }

    // Reads DWARF embedded in the test binary itself. Only Linux ships DWARF in
    // the executable; macOS keeps it in a separate .dSYM and Windows in a PDB.
    #[cfg(target_os = "linux")]
    #[test]
    fn object_symbol_resolver_reads_dwarf_from_local_elf() {
        // cargo-llvm-cov instruments the test binary and can make
        // self-symbolization resolve the anchor address to a nearby frame.
        if is_llvm_cov_run() {
            return;
        }
        let OwnExecutable {
            bytes,
            anchor_address: address,
        } = own_executable();
        let resolver = ObjectSymbolResolver::from_bytes(&bytes).unwrap();

        let frames = resolver
            .symbolize(&SymbolizeRequest {
                build_id: String::new(),
                filename: std::env::current_exe()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                address,
            })
            .unwrap();

        assert!(
            frames
                .iter()
                .any(|frame| frame.function.contains("object_symbol_anchor"))
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn byte_backed_object_symbol_resolver_reads_dwarf_locations() {
        // Skip under llvm-cov coverage instrumentation. This test self-symbolizes
        // the test binary and asserts the anchor resolves to its exact file+line.
        // Coverage instrumentation rewrites the binary's code and line tables, so
        // on some toolchains addr2line resolves the anchor's address to a frame
        // this assertion rejects (observed only on CI's `cargo llvm-cov nextest`
        // runner — it passes in every non-coverage Linux build, including a
        // faithful local llvm-cov+nextest repro). `LLVM_PROFILE_FILE` is set by
        // cargo-llvm-cov for the instrumented test process, so use it to detect a
        // coverage run; the test still runs in normal dev/CI builds.
        if is_llvm_cov_run() {
            return;
        }
        let OwnExecutable {
            bytes,
            anchor_address: address,
        } = own_executable();
        let resolver = ObjectSymbolResolver::from_bytes(&bytes).unwrap();

        let frames = resolver
            .symbolize(&SymbolizeRequest {
                build_id: "build-a".to_string(),
                filename: "/missing/on/disk".to_string(),
                address,
            })
            .unwrap();

        assert!(
            frames
                .iter()
                .any(|frame| frame.function.contains("object_symbol_anchor"))
        );
        let exe_path = std::env::current_exe().unwrap();
        let loader = addr2line::Loader::new(&exe_path).unwrap();
        let exe_has_file_line_dwarf = loader_frames(&loader, address)
            .is_some_and(|frames| frames.iter().any(is_object_symbol_anchor_location));
        if !exe_has_file_line_dwarf {
            return;
        }

        assert!(frames.iter().any(is_object_symbol_anchor_location));
    }

    #[cfg(target_os = "linux")]
    fn is_object_symbol_anchor_location(frame: &NativeSymbol) -> bool {
        frame.function.contains("object_symbol_anchor")
            && frame.file.ends_with("symbolizer.rs")
            && frame.line > 0
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn debuginfod_resolver_fetches_and_caches_build_id_artifact() {
        if is_llvm_cov_run() {
            return;
        }
        let OwnExecutable {
            bytes,
            anchor_address: address,
        } = own_executable();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let max_debuginfo = ByteSize::from_bytes(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let served = Arc::new(AtomicUsize::new(0));
        let served_clone = Arc::clone(&served);
        let server_thread = std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            let connection = accept_with_deadline(&listener);
            let AcceptedConnection { mut stream, .. } = connection;
            let mut request = [0_u8; 1024];
            let read = std::io::Read::read(&mut stream, &mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with("GET /buildid/deadbeef/debuginfo "));
            served_clone.fetch_add(1, Ordering::Relaxed);
            let header = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                bytes.len()
            );
            std::io::Write::write_all(&mut stream, header.as_bytes()).unwrap();
            std::io::Write::write_all(&mut stream, &bytes).unwrap();
        });
        let config = DebuginfodConfig::new(max_debuginfo, millis(250), secs(3)).unwrap();
        let resolver = DebuginfodResolver::with_config(vec![base_url], config).unwrap();

        let first = resolver
            .symbolize(&SymbolizeRequest {
                build_id: "deadbeef".to_string(),
                filename: "/missing/on/disk".to_string(),
                address,
            })
            .unwrap();
        let second = resolver
            .symbolize(&SymbolizeRequest {
                build_id: "deadbeef".to_string(),
                filename: "/missing/on/disk".to_string(),
                address,
            })
            .unwrap();

        server_thread.join().unwrap();
        check!(served.load(Ordering::Relaxed) == 1);
        check!(first == second);
        check!(
            first
                .iter()
                .any(|frame| frame.function.contains("object_symbol_anchor"))
        );
    }

    #[test]
    fn chained_resolver_falls_through_to_later_resolvers() {
        struct EmptyResolver;

        impl NativeResolver for EmptyResolver {
            fn symbolize(&self, _request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
                None
            }
        }

        let fixed = Arc::new(FixedResolver {
            calls: AtomicUsize::new(0),
            expected_address: 0x10,
        });
        let chain = ChainedResolver::new(vec![Arc::new(EmptyResolver), fixed.clone()]);

        let out = chain
            .symbolize(&SymbolizeRequest {
                build_id: "build-a".to_string(),
                filename: "/bin/app".to_string(),
                address: 0x10,
            })
            .unwrap();

        assert!(out[0].function == "native_main");
        assert!(fixed.calls.load(Ordering::Relaxed) == 1);
    }

    #[test]
    fn object_symbol_resolver_rejects_invalid_object_bytes() {
        let bytes = b"not an object file".to_vec();

        assert!(parse_object_guarded(&bytes).is_err());
        assert!(ObjectSymbolResolver::from_bytes(&bytes).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn file_system_resolver_reads_symbols_from_cached_file() {
        if is_llvm_cov_run() {
            return;
        }
        let exe = std::env::current_exe().unwrap();
        let OwnExecutable {
            anchor_address: address,
            ..
        } = own_executable();
        let resolver = FileSystemResolver::default();
        let request = SymbolizeRequest {
            build_id: String::new(),
            filename: exe.to_string_lossy().into_owned(),
            address,
        };

        let first = resolver.symbolize(&request).unwrap();
        let second = resolver.symbolize(&request).unwrap();

        assert!(
            first
                .iter()
                .any(|frame| frame.function.contains("object_symbol_anchor"))
        );
        assert!(first == second);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn nearest_symbol_name_handles_zero_size_and_end_boundaries() {
        let _ = object_symbol_anchor();
        let bytes = std::fs::read(std::env::current_exe().unwrap()).unwrap();
        let object = object::File::parse(bytes.as_slice()).unwrap();
        let symbols = object
            .symbols()
            .filter_map(|symbol| {
                let name = symbol.name().ok()?;
                (!name.is_empty()).then(|| SymbolRecord {
                    address: symbol.address(),
                    size: symbol.size(),
                    name: name.to_string(),
                })
            })
            .collect::<Vec<_>>();
        let candidates = symbols
            .iter()
            .filter(|symbol| symbol.address != 0 && symbol.size == 0)
            .find_map(|symbol| {
                let covered_by_sized = symbols.iter().any(|candidate| {
                    candidate.size != 0
                        && candidate.address <= symbol.address
                        && symbol.address < candidate.address.saturating_add(candidate.size)
                });
                if covered_by_sized {
                    return None;
                }
                let names = symbols
                    .iter()
                    .filter(|candidate| candidate.address == symbol.address && candidate.size == 0)
                    .map(|candidate| candidate.name.clone())
                    .collect::<Vec<_>>();
                (!names.is_empty()).then_some(ZeroSizeSymbols {
                    address: symbol.address,
                    names,
                })
            })
            .expect("test binary has an uncovered zero-size symbol");
        assert!(
            nearest_symbol_name(&object, candidates.address)
                .is_some_and(|name| candidates.names.iter().any(|candidate| candidate == &name))
        );

        let anchor = object
            .symbols()
            .find(|symbol| {
                symbol.address() != 0
                    && symbol.size() > 0
                    && symbol.name().is_ok_and(is_object_symbol_anchor_symbol)
            })
            .expect("anchor has a sized symbol");
        let at_end = nearest_symbol_name(&object, anchor.address() + anchor.size());
        assert!(!at_end.is_some_and(|name| name.contains("object_symbol_anchor")));
    }

    #[test]
    fn build_id_validation_accepts_lowercase_hex() {
        for build_id in [
            "deadbeef",
            "0123456789abcdef",
            // Real debuginfod build-ids are 40-char SHA-1 hex digests.
            "aabbccddeeff00112233445566778899aabbccdd",
            // Minimum length is two hex digits.
            "ab",
        ] {
            assert!(is_valid_build_id(build_id), "{build_id}");
        }
    }

    #[test]
    fn build_id_validation_rejects_traversal_and_non_hex() {
        for build_id in [
            // Path traversal and slashes must never reach URL construction.
            "../x",
            "a/b",
            "..",
            "foo/../bar",
            // Uppercase is not a valid lowercase-hex build-id.
            "DEADBEEF",
            "AbCd",
            // Empty / single char / non-hex bytes.
            "",
            "a",
            "xyz",
            "dead beef",
            "build-a",
        ] {
            assert!(!is_valid_build_id(build_id), "{build_id:?}");
        }
    }

    #[test]
    fn debuginfod_rejects_invalid_build_id_without_fetching() {
        // Point at an address that would refuse connections; an invalid
        // build_id must short-circuit before any network attempt is made.
        let resolver = DebuginfodResolver::new(vec!["http://127.0.0.1:1".to_string()]).unwrap();

        let out = resolver.symbolize(&SymbolizeRequest {
            build_id: "../etc/passwd".to_string(),
            filename: "/bin/app".to_string(),
            address: 0x10,
        });

        assert!(out.is_none());
    }

    #[test]
    fn debuginfod_build_url_pushes_segments_safely() {
        let base = reqwest::Url::parse("https://debuginfod.example/").unwrap();
        let url = DebuginfodResolver::build_url(&base, "deadbeef").unwrap();

        assert!(url.as_str() == "https://debuginfod.example/buildid/deadbeef/debuginfo");
        // Host is untouched and there is exactly one path beyond the prefix.
        assert!(url.host_str() == Some("debuginfod.example"));
    }

    #[test]
    fn debuginfod_build_url_keeps_existing_base_path() {
        let base = reqwest::Url::parse("https://proxy.example/debuginfod").unwrap();
        let url = DebuginfodResolver::build_url(&base, "abcd").unwrap();

        assert!(url.as_str() == "https://proxy.example/debuginfod/buildid/abcd/debuginfo");
    }

    #[test]
    fn read_capped_rejects_oversized_body() {
        // The size-cap logic must abort once the accumulated bytes exceed the
        // ceiling. Drive it through an in-memory reader to keep the test
        // cross-platform (no socket / Linux ELF needed).
        let cap: u64 = 1024;
        let cap_usize = usize::try_from(cap).unwrap();
        let oversized = vec![0_u8; cap_usize + 1];
        let out = read_capped_reader(&oversized[..], cap);
        assert!(out.is_none());

        let exact = vec![7_u8; cap_usize];
        let out = read_capped_reader(&exact[..], cap).unwrap();
        assert!(out.len() == cap_usize);
    }

    #[yare::parameterized(
        absent = { None, true },
        exact_limit = { Some(10), true },
        over_limit = { Some(11), false },
    )]
    fn content_length_cap_allows_absent_and_exact_lengths_only(
        content_length: Option<u64>,
        want: bool,
    ) {
        assert2::assert!(
            content_length_within_cap(content_length, 10) == want,
            "{content_length:?}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn debuginfod_does_not_follow_redirects() {
        // A debuginfod server that 302-redirects must NOT be followed: this is
        // a core SSRF-pivot defence. Serve a redirect to an alternate path on
        // the same listener and assert the resolver gives up (returns None)
        // rather than chasing the Location header.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let followed = Arc::new(AtomicUsize::new(0));
        let followed_clone = Arc::clone(&followed);
        let server_thread = std::thread::spawn(move || {
            // First (and only expected) request: reply with a redirect.
            listener
                .set_nonblocking(true)
                .expect("set listener non-blocking");
            let connection = accept_with_deadline(&listener);
            let AcceptedConnection { mut stream, .. } = connection;
            let mut request = [0_u8; 1024];
            let _ = std::io::Read::read(&mut stream, &mut request).unwrap();
            let response = "HTTP/1.1 302 Found\r\nlocation: /elsewhere\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
            std::io::Write::write_all(&mut stream, response.as_bytes()).unwrap();
            // If the client wrongly follows the redirect, a second connection
            // arrives; record it.
            std::thread::sleep(std::time::Duration::from_millis(200));
            if listener.accept().is_ok() {
                followed_clone.fetch_add(1, Ordering::Relaxed);
            }
        });

        let resolver = DebuginfodResolver::new(vec![base_url]).unwrap();
        let out = resolver.symbolize(&SymbolizeRequest {
            build_id: "deadbeef".to_string(),
            filename: "/missing/on/disk".to_string(),
            address: 0x10,
        });

        server_thread.join().unwrap();
        assert!(out.is_none());
        assert!(followed.load(Ordering::Relaxed) == 0);
    }

    /// The test binary's own bytes, and the address of
    /// `object_symbol_anchor` in them.
    #[cfg(target_os = "linux")]
    struct OwnExecutable {
        bytes: Vec<u8>,
        anchor_address: u64,
    }

    #[cfg(target_os = "linux")]
    fn own_executable() -> OwnExecutable {
        let _ = object_symbol_anchor();
        let bytes = std::fs::read(std::env::current_exe().unwrap()).unwrap();
        let anchor_address = object_symbol_anchor_address(&bytes);
        OwnExecutable {
            bytes,
            anchor_address,
        }
    }

    #[cfg(target_os = "linux")]
    fn object_symbol_anchor_address(bytes: &[u8]) -> u64 {
        let object = object::File::parse(bytes).unwrap();
        object
            .symbols()
            .find(|symbol| {
                symbol.address() != 0 && symbol.name().is_ok_and(is_object_symbol_anchor_symbol)
            })
            .unwrap()
            .address()
    }

    #[cfg(target_os = "linux")]
    fn is_object_symbol_anchor_symbol(name: &str) -> bool {
        name.ends_with("object_symbol_anchor") || name.contains("object_symbol_anchor17h")
    }

    #[cfg(target_os = "linux")]
    struct AcceptedConnection {
        stream: std::net::TcpStream,
    }

    fn accept_with_deadline(listener: &std::net::TcpListener) -> AcceptedConnection {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match listener.accept() {
                Ok(connection) => {
                    return AcceptedConnection {
                        stream: connection.0,
                    };
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "timed out waiting for debuginfod request"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(err) => panic!("accept failed: {err}"),
            }
        }
    }

    // Anchor symbol the Linux-only DWARF tests locate in the test binary.
    #[cfg(target_os = "linux")]
    #[inline(never)]
    fn object_symbol_anchor() -> u64 {
        std::hint::black_box(42)
    }
}

mod artifact_cache;
mod chained_resolver;
mod debuginfod_config;
mod debuginfod_resolver;
mod file_system_resolver;
mod lazy_symbolizer;
mod loader_frames;
mod object_symbol_resolver;
use artifact_cache::ArtifactCache;
pub use chained_resolver::ChainedResolver;
pub use debuginfod_config::DebuginfodConfig;
pub use debuginfod_resolver::DebuginfodResolver;
pub use file_system_resolver::FileSystemResolver;
pub use lazy_symbolizer::LazySymbolizer;
use loader_frames::loader_frames;
pub use object_symbol_resolver::ObjectSymbolResolver;
/// Default time allowed to open a debuginfod connection.
pub const DEFAULT_DEBUGINFOD_CONNECT_TIMEOUT: Time = secs(5);

/// Default maximum size of a debuginfo artifact downloaded from a debuginfod
/// server.
pub const DEFAULT_DEBUGINFOD_MAX_ARTIFACT_SIZE: ByteSize = mebibytes(512);

/// Default time allowed for a whole debuginfod request, connection included.
pub const DEFAULT_DEBUGINFOD_REQUEST_TIMEOUT: Time = secs(10);

pub(crate) fn content_length_within_cap(content_length: Option<u64>, cap: u64) -> bool {
    content_length.is_none_or(|len| len <= cap)
}

/// Returns `true` if and only if `build_id` is a valid debuginfod build-id.
///
/// A valid build-id is a non-empty lowercase hex string. debuginfod build-ids
/// are hex digests, so this function rejects a build-id that holds `/`, `.`,
/// `..`, uppercase, or other bytes. The rejection happens before the build-id
/// can go into a URL. This is a defence against SSRF and path traversal.
pub(crate) fn is_valid_build_id(build_id: &str) -> bool {
    build_id.len() >= 2
        && build_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Recover a poisoned mutex rather than propagate the panic.
///
/// One panicked worker must not permanently `DoS` the resolver. This function
/// takes ownership of the inner guard and continues.
pub(crate) fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Read an HTTP body into memory.
///
/// This function stops and returns `None` as soon as the accumulated size is
/// more than `cap` bytes. It avoids the unbounded `response.bytes()`
/// allocation.
pub(crate) fn read_capped(mut response: reqwest::blocking::Response, cap: u64) -> Option<Vec<u8>> {
    read_capped_reader(&mut response, cap)
}

pub(crate) fn validate_positive_timeout(name: &str, timeout: Time) -> Result<(), String> {
    let duration = std::time::Duration::try_from_secs_f64(timeout.secs_f64())
        .map_err(|error| format!("debuginfod {name} timeout: {error}"))?;
    if duration.is_zero() {
        return Err(format!("debuginfod {name} timeout must be positive"));
    }
    Ok(())
}

/// Parse an untrusted ELF/DWARF blob with `object::File::parse`.
///
/// This function catches any panic that the parser can trigger on a crafted
/// artifact. It returns `Ok(())` only when the bytes parse cleanly and the
/// parser does not panic.
pub(crate) fn parse_object_guarded(bytes: &[u8]) -> Result<(), String> {
    std::panic::catch_unwind(|| {
        object::File::parse(bytes)
            .map(|_| ())
            .map_err(|err| err.to_string())
    })
    .unwrap_or_else(|_| Err("panic while parsing object file".to_string()))
}

pub(crate) fn read_capped_reader(mut reader: impl Read, cap: u64) -> Option<Vec<u8>> {
    let cap_usize = usize::try_from(cap).unwrap_or(usize::MAX);
    let mut buf = Vec::new();
    let mut chunk = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut chunk).ok()?;
        if read == 0 {
            break;
        }
        if buf.len().saturating_add(read) > cap_usize {
            return None;
        }
        buf.extend_from_slice(&chunk[..read]);
    }
    Some(buf)
}
