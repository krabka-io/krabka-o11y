use super::{MetricBlockKind, Path, compaction_index_key, escape_object_path_segment};

/// How many hex digits of the content hash a Parquet key holds.
const KEY_HASH_DIGITS: usize = 16;

/// The object names of the TSDB imports of one tenant.
///
/// A Parquet key holds the block ULID and a prefix of the content hash. Two
/// uploads of one ULID with different content therefore never write the same
/// object, and two uploads of the same ULID and content write the same rows to
/// the same object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TsdbImportKeys {
    records: String,
    objects: String,
}

impl TsdbImportKeys {
    pub fn new(tenant: &str, manifest_prefix: &str, record_prefix: &str) -> Self {
        let tenant = escape_object_path_segment(tenant);
        Self {
            records: join(record_prefix, &tenant),
            objects: join(manifest_prefix, &tenant),
        }
    }

    /// The binding that ties `ulid` to one content hash.
    pub fn binding(&self, ulid: &str) -> Path {
        Path::from(format!("{}/{ulid}/import.json", self.records))
    }

    /// The import record whose creation commits the content `sha256`.
    pub fn record(&self, sha256: &str) -> Path {
        Path::from(format!("{}/by-sha256/{sha256}.json", self.records))
    }

    /// The Parquet block and `.index` manifest keys of one import object.
    pub fn object(&self, ulid: &str, sha256: &str, kind: MetricBlockKind) -> (String, String) {
        let digits = sha256.get(..KEY_HASH_DIGITS).unwrap_or(sha256);
        let block_key = format!(
            "{}/uploaded/{ulid}-{digits}/{}.parquet",
            self.objects,
            kind.object_path()
        );
        let index_key = compaction_index_key(&block_key);
        (block_key, index_key)
    }
}

fn join(prefix: &str, tenant: &str) -> String {
    let prefix = prefix.trim_end_matches('/');
    if prefix.is_empty() {
        tenant.to_owned()
    } else {
        format!("{prefix}/{tenant}")
    }
}
