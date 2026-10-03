use super::{BTreeSet, TsdbImportKeys};

/// The objects of one listing of the metrics prefix that make up the metrics
/// index, split by whether queries read them.
///
/// A `.index` manifest outside a TSDB import directory is live when it
/// exists. A manifest in an import directory is live only when the same
/// listing also holds the publication marker of that directory. The import
/// writes the marker after every manifest of the import, so a listing finds
/// all the manifests of an import live, or none of them.
///
/// The split needs no request of its own, because the marker is in the
/// listing that finds the manifests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionIndexListing<T> {
    /// The manifests that queries read, in listing order.
    pub live: Vec<T>,
    /// The manifests in import directories that have no marker.
    pub pending: Vec<T>,
    /// The publication markers of import directories.
    pub markers: Vec<T>,
}

impl<T> CompactionIndexListing<T> {
    /// Splits `objects`, whose keys `key` gives, and drops each object that is
    /// not a manifest or a marker.
    pub fn new(objects: Vec<T>, key: impl Fn(&T) -> &str) -> Self {
        let published: BTreeSet<String> = objects
            .iter()
            .filter_map(|object| marker_directory(key(object)).map(str::to_owned))
            .collect();
        let mut listing = Self {
            live: Vec::new(),
            pending: Vec::new(),
            markers: Vec::new(),
        };
        for object in objects {
            let target = {
                let key = key(&object);
                if marker_directory(key).is_some() {
                    Some(&mut listing.markers)
                } else if !is_index(key) {
                    None
                } else if key.rsplit_once('/').is_some_and(|(directory, _)| {
                    TsdbImportKeys::is_import_directory(directory) && !published.contains(directory)
                }) {
                    Some(&mut listing.pending)
                } else {
                    Some(&mut listing.live)
                }
            };
            if let Some(target) = target {
                target.push(object);
            }
        }
        listing
    }
}

/// The import directory of `key` if `key` is a publication marker.
fn marker_directory(key: &str) -> Option<&str> {
    key.rsplit_once('/')
        .filter(|(directory, name)| {
            *name == TsdbImportKeys::PUBLISHED_MARKER
                && TsdbImportKeys::is_import_directory(directory)
        })
        .map(|(directory, _)| directory)
}

/// Whether `key` names a manifest: its extension is `index` in any ASCII
/// case.
fn is_index(key: &str) -> bool {
    std::path::Path::new(key)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("index"))
}
