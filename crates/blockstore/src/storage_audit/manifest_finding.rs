use super::{StorageFinding, StorageFindingKind, StorageSignal};

/// The finding for an index or manifest that did not read.
///
/// A version this build does not read is `unsupported_format`. Anything else
/// is `unreadable_manifest`.
pub fn manifest_finding(
    signal: StorageSignal,
    tenant: Option<String>,
    path: impl Into<String>,
    detail: &str,
) -> StorageFinding {
    let unsupported = detail.contains("manifest version");
    let kind = if unsupported {
        StorageFindingKind::UnsupportedFormat
    } else {
        StorageFindingKind::UnreadableManifest
    };
    StorageFinding::new(kind, signal, tenant, path, detail)
}
