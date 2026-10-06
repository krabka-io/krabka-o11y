//! Generates Connect-RPC server stubs and prost message types from the vendored
//! Pyroscope-compatible Connect and OTLP profile protos.
//!
//! This script drives codegen through the vendored `protoc` binary from
//! `protoc-bin-vendored`, so the build is hermetic. It needs no system `protoc`
//! and no network fetch. The Connect generator `connectrpc-axum-build` always
//! invokes a `protoc` binary, so this script supplies the vendored one through
//! the `protoc_executable` option of `prost-build`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/adhocprofiles/v1/adhocprofiles.proto",
        "proto/capabilities/v1/feature_flags.proto",
        "proto/debuginfo/v1alpha1/debuginfo.proto",
        "proto/google/v1/profile.proto",
        "proto/push/v1/push.proto",
        "proto/querier/v1/querier.proto",
        "proto/settings/v1/settings.proto",
        "proto/settings/v1/recording_rules.proto",
        "proto/opentelemetry/proto/collector/profiles/v1development/profiles_service.proto",
    ];
    let includes = ["proto"];
    let protoc_path = protoc_path()?;
    connectrpc_axum_build::compile_protos(&protos, &includes)
        .with_prost_config(move |config| {
            config.protoc_executable(protoc_path.clone());
        })
        .with_pbjson_config(|config| {
            config.ignore_unknown_fields();
        })
        .compile()?;
    normalize_generated_code()?;
    for path in [
        "proto/adhocprofiles/v1/adhocprofiles.proto",
        "proto/capabilities/v1/feature_flags.proto",
        "proto/debuginfo/v1alpha1/debuginfo.proto",
        "proto/types/v1/types.proto",
        "proto/google/v1/profile.proto",
        "proto/push/v1/push.proto",
        "proto/querier/v1/querier.proto",
        "proto/settings/v1/settings.proto",
        "proto/settings/v1/recording_rules.proto",
        "proto/opentelemetry/proto/common/v1/common.proto",
        "proto/opentelemetry/proto/resource/v1/resource.proto",
        "proto/opentelemetry/proto/profiles/v1development/profiles.proto",
        "proto/opentelemetry/proto/collector/profiles/v1development/profiles_service.proto",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    Ok(())
}

fn normalize_generated_code() -> Result<(), Box<dyn std::error::Error>> {
    const GENERATED_FILES: &[&str] = &[
        "adhocprofiles.v1.rs",
        "capabilities.v1.rs",
        "debuginfo.v1alpha1.rs",
        "google.v1.rs",
        "opentelemetry.proto.collector.profiles.v1development.rs",
        "opentelemetry.proto.common.v1.rs",
        "opentelemetry.proto.profiles.v1development.rs",
        "opentelemetry.proto.resource.v1.rs",
        "push.v1.rs",
        "querier.v1.rs",
        "settings.v1.rs",
        "types.v1.rs",
    ];
    const BUILDERS: &[&str] = &[
        "AdHocProfileServiceBuilder",
        "DebuginfoServiceBuilder",
        "FeatureFlagsServiceBuilder",
        "ProfilesServiceBuilder",
        "PusherServiceBuilder",
        "QuerierServiceBuilder",
        "SettingsServiceBuilder",
        "RecordingRulesServiceBuilder",
    ];

    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    for filename in GENERATED_FILES {
        let path = out_dir.join(filename);
        let source = std::fs::read_to_string(&path)?;
        let normalized = krabka_codegen::strip_documentation(&source)?;
        let normalized = krabka_codegen::normalize_pbjson(&normalized)?;
        let normalized = if *filename == "google.v1.rs" {
            normalized.replace(
                "pub struct Mapping {",
                "#[allow(clippy::struct_excessive_bools)]\npub struct Mapping {",
            )
        } else {
            normalized
        };

        let mut normalized = krabka_codegen::annotate_must_use(
            &normalized,
            krabka_codegen::MustUse::EnumNames,
            BUILDERS,
        )?;
        if *filename == "google.v1.rs" {
            normalized = krabka_codegen::compact_impl(&normalized, "Deserialize", "Profile")?;
        }
        std::fs::write(path, normalized)?;
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
