/// Version of the storage audit report, the repair report and the repair
/// audit log.
///
/// A reader refuses a report or a log line that carries a different version,
/// so a report from a newer build cannot drive a repair in an older one.
pub const STORAGE_AUDIT_SCHEMA_VERSION: u32 = 1;
