use krabka_client_consumer::ConsumerRecord;
use sha2::{Digest, Sha256};

/// The topic and payload distinguish reused offsets from unrelated WAL data.
#[derive(Clone, Copy)]
pub(crate) struct WalPosition {
    pub(crate) partition: i32,
    pub(crate) offset: i64,
    pub(crate) record_hash: [u8; 32],
}

impl WalPosition {
    pub(crate) fn from_record(record: &ConsumerRecord, value: &[u8]) -> Self {
        let mut hash = Sha256::new();
        hash.update(record.topic.as_bytes());
        hash.update([0]);
        hash.update(value);
        Self {
            partition: record.partition,
            offset: record.offset,
            record_hash: hash.finalize().into(),
        }
    }

    pub(crate) fn sample_identity(self, ordinal: u64) -> Vec<u8> {
        [
            self.record_hash.as_slice(),
            self.partition.to_be_bytes().as_slice(),
            self.offset.to_be_bytes().as_slice(),
            ordinal.to_be_bytes().as_slice(),
        ]
        .concat()
    }
}
