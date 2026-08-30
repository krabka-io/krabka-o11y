use super::{BTreeSet, BTreeMap, emit_warning, mixed_classic_native_warning};

/// Emits one `MixedClassicNativeHistogramsWarning` per mixed group key.
///
/// A mixed group key held both a classic and a native histogram for the same
/// label set.
pub(crate) fn warn_mixed_histograms(
    mixed_keys: &BTreeSet<String>,
    names: &BTreeMap<String, String>,
) {
    for key in mixed_keys {
        let metric = names.get(key).map_or("", String::as_str);
        emit_warning(mixed_classic_native_warning(metric));
    }
}
