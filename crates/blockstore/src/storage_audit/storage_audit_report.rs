use super::{
    BTreeMap, Deserialize, STORAGE_AUDIT_SCHEMA_VERSION, Serialize, StorageAuditError,
    StorageAuditScope, StorageFinding, StorageFindingKind, StorageFindingSeverity,
};

/// The machine-readable result of one read-only storage audit.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StorageAuditReport {
    /// [`STORAGE_AUDIT_SCHEMA_VERSION`] of the build that wrote the report.
    pub schema_version: u32,
    /// Always `true`. An audit only lists and reads.
    pub read_only: bool,
    pub scope: StorageAuditScope,
    /// Every object the listing returned, in scope or not.
    pub objects_listed: usize,
    /// Listed objects that match no key grammar the audit knows.
    pub objects_unclassified: usize,
    /// Sorted and free of duplicates.
    pub findings: Vec<StorageFinding>,
    /// The number of findings of each kind. A kind with no finding is absent.
    pub counts: BTreeMap<StorageFindingKind, usize>,
}

impl StorageAuditReport {
    /// A report over `findings`. Sorts them, drops duplicates and counts
    /// them.
    #[must_use]
    pub fn new(
        scope: StorageAuditScope,
        objects_listed: usize,
        objects_unclassified: usize,
        mut findings: Vec<StorageFinding>,
    ) -> Self {
        findings.sort();
        findings.dedup();
        let mut counts = BTreeMap::new();
        for finding in &findings {
            *counts.entry(finding.kind).or_insert(0) += 1;
        }
        Self {
            schema_version: STORAGE_AUDIT_SCHEMA_VERSION,
            read_only: true,
            scope,
            objects_listed,
            objects_unclassified,
            findings,
            counts,
        }
    }

    /// Whether any finding is damage rather than a warning.
    #[must_use]
    pub fn has_damage(&self) -> bool {
        self.findings
            .iter()
            .any(|finding| finding.severity == StorageFindingSeverity::Damage)
    }

    /// Decodes a report and refuses one from another schema version.
    ///
    /// # Errors
    /// Returns [`StorageAuditError::UnsupportedSchemaVersion`] for a report of
    /// another version, and [`StorageAuditError::InvalidReport`] for bytes that
    /// are not a report.
    pub fn from_json(bytes: &[u8]) -> Result<Self, StorageAuditError> {
        #[derive(Deserialize)]
        struct VersionProbe {
            schema_version: u32,
        }
        let probe: VersionProbe = serde_json::from_slice(bytes)
            .map_err(|error| StorageAuditError::InvalidReport(error.to_string()))?;
        if probe.schema_version != STORAGE_AUDIT_SCHEMA_VERSION {
            return Err(StorageAuditError::UnsupportedSchemaVersion {
                actual: probe.schema_version,
                expected: STORAGE_AUDIT_SCHEMA_VERSION,
            });
        }
        serde_json::from_slice(bytes)
            .map_err(|error| StorageAuditError::InvalidReport(error.to_string()))
    }
}
