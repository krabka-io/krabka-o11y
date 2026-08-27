//! Generates prost message types from the crate's protos.
//!
//! The set holds the vendored `remote_write` v1 and v2 surfaces and the Crabka
//! clock confidence signal.
//!
//! This script drives codegen through a vendored `protoc` binary,
//! `protoc-bin-vendored`, so the build is hermetic. It needs no system
//! `protoc`, no network fetch, and no platform-specific protobuf release
//! archive naming.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/prometheus/remote.proto",
        "proto/io/prometheus/write/v2/types.proto",
        "proto/krabka/clocks/v1/clocks.proto",
    ];
    let includes = ["proto"];

    let mut config = prost_build::Config::new();
    let protoc_path = protoc_path()?;
    config.protoc_executable(protoc_path);

    config.compile_protos(&protos, &includes)?;
    rewrite_generated_enums()?;
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    Ok(())
}

fn rewrite_generated_enums() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    for file in [
        "prometheus.rs",
        "io.prometheus.write.v2.rs",
        "krabka.clocks.v1.rs",
    ] {
        let path = out_dir.join(file);
        let generated = std::fs::read_to_string(&path)?;
        let rewritten = generated
            .replace("the ProtoBuf definition", "the `ProtoBuf` definition")
            .replace(
                "\n        pub fn as_str_name(&self)",
                "\n        #[must_use]\n        pub fn as_str_name(&self)",
            )
            .replace(
                "\n    pub fn as_str_name(&self)",
                "\n    #[must_use]\n    pub fn as_str_name(&self)",
            )
            .replace(
                "\n        pub fn from_str_name(value: &str)",
                "\n        #[must_use]\n        pub fn from_str_name(value: &str)",
            )
            .replace(
                "\n    pub fn from_str_name(value: &str)",
                "\n    #[must_use]\n    pub fn from_str_name(value: &str)",
            );
        std::fs::write(path, rewritten)?;
    }
    Ok(())
}

/// The `protoc` this build should drive.
///
/// A build system that ships its own hermetic `protoc` says so through
/// `PROTOC`, and that one wins: the vendored crates locate their binary
/// through `env!("CARGO_MANIFEST_DIR")`, which bakes an absolute build path
/// into the artifact and so cannot be built reproducibly. Cargo sets nothing,
/// falls through, and uses the vendored binary exactly as before.
fn protoc_path() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
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
