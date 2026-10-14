#[cfg(feature = "experimental-functions")]
use super::Labels;

/// Hashes labels exactly like Prometheus' `labels.Labels.Hash`.
///
/// Krabka's persisted series fingerprint deliberately uses a different,
/// length-prefixed encoding. `PromQL`'s `limit_ratio`, however, is externally
/// observable and must use Prometheus' xxHash64 over sorted
/// `name\xffvalue\xff` pairs.
#[cfg(feature = "experimental-functions")]
pub(crate) fn prometheus_labels_hash(labels: &Labels) -> u64 {
    let capacity = labels
        .iter()
        .map(|pair| pair.name.len() + pair.value.len() + 2)
        .sum();
    let mut bytes = Vec::with_capacity(capacity);
    for pair in labels.iter() {
        bytes.extend_from_slice(pair.name.as_bytes());
        bytes.push(0xff);
        bytes.extend_from_slice(pair.value.as_bytes());
        bytes.push(0xff);
    }
    xxhash_rust::xxh64::xxh64(&bytes, 0)
}
