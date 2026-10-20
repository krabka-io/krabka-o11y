//! The `protoc` a crate's build script drives.
//!
//! `//crates/metrics` and `//crates/profiles` reach this file from their
//! `build.rs` with `#[path]`, so it depends only on the standard library and,
//! behind the includer's `vendored-protoc` feature, on `protoc-bin-vendored`.

/// The `protoc` this build should drive.
///
/// A build system that ships its own hermetic `protoc` says so through
/// `PROTOC`, and that one wins: the vendored crates locate their binary
/// through `env!("CARGO_MANIFEST_DIR")`, which bakes an absolute build path
/// into the artifact and so cannot be built reproducibly. Cargo sets nothing,
/// falls through, and uses the vendored binary exactly as before.
pub fn protoc_path() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    if let Some(from_toolchain) = std::env::var_os("PROTOC") {
        return Ok(std::path::PathBuf::from(from_toolchain));
    }
    #[cfg(feature = "vendored-protoc")]
    {
        Ok(protoc_bin_vendored::protoc_bin_path()?)
    }
    #[cfg(not(feature = "vendored-protoc"))]
    {
        Err("no PROTOC in the environment and the vendored protoc is disabled".into())
    }
}
