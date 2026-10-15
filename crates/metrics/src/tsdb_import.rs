//! Decoder and validator for native Prometheus TSDB blocks.
//!
//! [`decode_tsdb_block`] reads a block that Prometheus, Mimir or Thanos wrote
//! (the index, the chunk segments and the tombstones), checks it against the
//! format rules and the checksums, and returns the rows that the compactor
//! block writer accepts. It writes nothing. The upload route publishes the
//! rows only after this function succeeds, so a corrupt block never reaches
//! the store.
//!
//! A TSDB block holds float and histogram samples only. It carries no
//! exemplars and no metric metadata, so the decoded rows hold none.
//!
//! [`publish_tsdb_import`] writes the decoded rows as metric blocks, then
//! creates the import record of the block content as the commit point, then
//! writes the manifests, then makes all of them live with one publication
//! marker. [`tsdb_block_sha256`] names that content.

use std::{collections::BTreeMap, sync::Arc};

use arrow::record_batch::RecordBatch;
use krabka_blockstore::{BlockStoreError, BlockWriter, escape_object_path_segment};
use num_traits::ToPrimitive;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, path::Path};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    BucketSpan, CompactionIndexError, CompactionIndexManifest, CompactionObjectPlan, FloatRow,
    HistogramCodecError, MetricBlockKind, MetricLabelPair, MetricLabels as Labels, MetricString,
    NativeHistogram, NativeHistogramRow, ResetHint, TenantBatches, TenantCompactionRows,
    compactor::{compaction_index_key, series_labels_for_kind},
    encode_tenant_batches,
    wire::validate_spans_and_counts,
};

mod bit_reader;
mod byte_reader;
mod chunk_types;
mod chunk_segments;
mod block_reader;
mod block_types;
mod xor_chunk;
mod histogram_chunks;
mod import_commit;
mod publish_tsdb_import;
mod read_import_json;
mod tombstones;
mod tsdb_block_sha256;
mod tsdb_import_error;
mod tsdb_import_keys;
mod tsdb_import_limits;
mod publish_types;
mod tsdb_publish_error;

pub(crate) use self::tsdb_import_keys::TsdbImportKeys;
use self::{
    bit_reader::BitReader,
    xor_chunk::{decode_xor_chunk, XorState},
    histogram_chunks::{decode_float_histogram_chunk, decode_histogram_chunk},
    byte_reader::ByteReader,
    chunk_segments::ChunkSegments,
    import_commit::ImportCommit,
    read_import_json::read_import_json,
    tombstones::Tombstones,
};
use self::chunk_types::{
    ChunkEncoding, ChunkError, ChunkMeta, ChunkSamples, TimestampedFloatSample,
    TimestampedHistogramSample,
};
pub use self::{
    publish_tsdb_import::publish_tsdb_import, tsdb_block_sha256::tsdb_block_sha256,
    tsdb_import_error::TsdbImportError, tsdb_import_limits::TsdbImportLimits,
    tsdb_publish_error::TsdbPublishError,
};
pub use self::block_types::{DecodedTsdbBlock, TsdbBlockFiles, TsdbBlockMeta, TsdbImportStats};
pub use self::publish_types::{
    TsdbImportBinding, TsdbImportObject, TsdbImportOutcome, TsdbImportRecord, TsdbImportTarget,
};
pub use self::block_reader::decode_tsdb_block;

/// The bit pattern Prometheus writes for a stale marker.
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// Converts an `i64` to the nearest `f64`, as Go's `float64(int64)` does.
pub fn i64_to_f64(value: i64) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}

/// Converts a `u64` to the nearest `f64`, as Go's `float64(uint64)` does.
pub fn u64_to_f64(value: u64) -> f64 {
    value.to_f64().unwrap_or(f64::MAX)
}

/// Reads a section framed as a big-endian `u32` length, the content, and a
/// big-endian CRC32C of the content, and returns the verified content.
///
/// The symbol table and the postings offset table use this frame.
pub fn checked_section<'a>(
    file: &'a [u8],
    offset: u64,
    section: &'static str,
) -> Result<&'a [u8], TsdbImportError> {
    let start = usize::try_from(offset)
        .ok()
        .filter(|start| *start <= file.len())
        .ok_or(TsdbImportError::Truncated { section })?;
    let mut reader = ByteReader::new(&file[start..], section);
    let length =
        usize::try_from(reader.be32()?).map_err(|_| TsdbImportError::Truncated { section })?;
    let content = reader.bytes(length)?;
    let expected = reader.be32()?;
    if crc32c::crc32c(content) != expected {
        return Err(TsdbImportError::Checksum {
            section: section.to_owned(),
        });
    }
    Ok(content)
}

/// Creates `value` as JSON at `key`, and answers `false` if the key exists.
pub async fn create_import_json<T: Serialize>(
    store: &dyn ObjectStore,
    key: &Path,
    value: &T,
) -> Result<bool, TsdbPublishError> {
    let bytes = serde_json::to_vec(value).map_err(|error| TsdbPublishError::InvalidRecord {
        key: key.to_string(),
        reason: error.to_string(),
    })?;
    match store
        .put_opts(
            key,
            PutPayload::from(bytes),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(source) => Err(TsdbPublishError::ObjectStore {
            key: key.to_string(),
            source,
        }),
    }
}

/// Writes `value` as JSON at `key`, and replaces an object at the key.
pub async fn put_import_json<T: Serialize>(
    store: &dyn ObjectStore,
    key: &Path,
    value: &T,
) -> Result<(), TsdbPublishError> {
    let bytes = serde_json::to_vec(value).map_err(|error| TsdbPublishError::InvalidRecord {
        key: key.to_string(),
        reason: error.to_string(),
    })?;
    store
        .put(key, PutPayload::from(bytes))
        .await
        .map_err(|source| TsdbPublishError::ObjectStore {
            key: key.to_string(),
            source,
        })?;
    Ok(())
}

/// The histogram that Prometheus reads for a stale marker in a histogram
/// chunk: only the sum is set, to the stale NaN.
pub fn stale_histogram(is_float: bool) -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        is_float,
        reset_hint: ResetHint::Unknown,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count: 0.0,
        sum: f64::from_bits(STALE_NAN_BITS),
        positive_spans: Vec::new(),
        positive_counts: Vec::new(),
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: None,
        start_timestamp_ms: None,
    }
}
