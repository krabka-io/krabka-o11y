//! Checkpoint codecs for rebuildable metrics-generator state.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn checkpoint_key_round_trips() {
        let trace = [0x22; 16];
        let key = encode_checkpoint_key("tenant-a", &trace, &[0xAA, 0xBB]);
        let parsed = parse_checkpoint_key(&key).unwrap();

        assert2::assert!(parsed == ("tenant-a".to_string(), trace, vec![0xAA, 0xBB]));
    }

    #[test]
    fn checkpoint_key_rejects_truncated_bytes() {
        let trace = [0x22; 16];
        let key = encode_checkpoint_key("tenant-a", &trace, &[0xAA, 0xBB]);
        let truncated = &key[..key.len() - 1];

        assert2::assert!(matches!(
            parse_checkpoint_key(truncated),
            Err(CheckpointCodecError::Truncated)
        ));
    }

    #[test]
    fn in_memory_store_round_trips_tombstones_and_isolates_tenants() {
        let store = InMemoryCheckpointStore::default();
        store.save("t", b"k1", b"v1");
        store.save("t", b"k2", b"v2");

        let all = store.load_all("t");
        assert2::assert!(all.len() == 2);

        store.save("t", b"k1", b"");
        let after_tombstone = store.load_all("t");
        assert2::assert!(after_tombstone == vec![(b"k2".to_vec(), b"v2".to_vec())]);
        check!(store.load_all("other").is_empty());
        check!(store.tenants() == vec!["t".to_string()]);
    }
}

mod checkpoint_codec_error;
mod edge_checkpoint_store;
mod encode_checkpoint_key;
mod get_bytes;
mod in_memory_checkpoint_store;
mod parse_checkpoint_key;
mod put_bytes;
mod store_key;

pub use checkpoint_codec_error::CheckpointCodecError;
pub use edge_checkpoint_store::EdgeCheckpointStore;
pub use encode_checkpoint_key::encode_checkpoint_key;
use get_bytes::get_bytes;
pub use in_memory_checkpoint_store::InMemoryCheckpointStore;
pub use parse_checkpoint_key::parse_checkpoint_key;
use put_bytes::put_bytes;
use store_key::StoreKey;
