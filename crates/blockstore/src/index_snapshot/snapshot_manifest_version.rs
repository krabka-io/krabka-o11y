/// Version of the snapshot-manifest encoding this build writes and accepts.
///
/// One version, no fallback: Krabka is greenfield, so a manifest written by an
/// older build is deleted, not migrated.
pub(crate) const SNAPSHOT_MANIFEST_VERSION: u32 = 1;
