/// The largest delete marker, erasure request or frontier object the audit
/// reads. Each one is a small JSON document, so a larger object is damage.
pub const MAX_STATE_OBJECT_BYTES: u64 = 16 * 1024 * 1024;
