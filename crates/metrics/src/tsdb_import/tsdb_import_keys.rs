use super::{MetricBlockKind, Path, compaction_index_key, escape_object_path_segment};

/// How many hex digits of the content hash a Parquet key holds.
const KEY_HASH_DIGITS: usize = 16;

/// The object names of the TSDB imports of one tenant.
///
/// A Parquet key holds the block ULID and a prefix of the content hash. Two
/// uploads of one ULID with different content therefore never write the same
/// object, and two uploads of the same ULID and content write the same rows to
/// the same object.
///
/// The blocks and manifests of one import share the directory
/// `<manifest_prefix>/<tenant>/uploaded/<ULID>-<hash>/`. The publication
/// marker in that directory makes its manifests live. See
/// [`CompactionIndexListing`](crate::CompactionIndexListing).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TsdbImportKeys {
    records: String,
    objects: String,
}

impl TsdbImportKeys {
    /// The name of the publication marker in an import directory.
    pub const PUBLISHED_MARKER: &str = "_published";

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
        let block_key = format!(
            "{}/{}.parquet",
            self.directory(ulid, sha256),
            kind.object_path()
        );
        let index_key = compaction_index_key(&block_key);
        (block_key, index_key)
    }

    /// The marker whose creation makes every manifest of one import live.
    pub fn published_marker(&self, ulid: &str, sha256: &str) -> Path {
        Path::from(format!(
            "{}/{}",
            self.directory(ulid, sha256),
            Self::PUBLISHED_MARKER
        ))
    }

    /// Whether `directory` has the shape of an import directory: a parent
    /// that ends with an `uploaded` segment, then `<ULID>-<hash>` with a
    /// 26-character upper-case ULID and 16 lower-case hex digits.
    pub fn is_import_directory(directory: &str) -> bool {
        let Some((parent, name)) = directory.rsplit_once('/') else {
            return false;
        };
        let Some((ulid, digits)) = name.split_once('-') else {
            return false;
        };
        parent.ends_with("/uploaded")
            && ulid.len() == 26
            && ulid
                .bytes()
                .all(|byte| byte.is_ascii_digit() || byte.is_ascii_uppercase())
            && digits.len() == KEY_HASH_DIGITS
            && digits
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }

    fn directory(&self, ulid: &str, sha256: &str) -> String {
        let digits = sha256.get(..KEY_HASH_DIGITS).unwrap_or(sha256);
        format!("{}/uploaded/{ulid}-{digits}", self.objects)
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
