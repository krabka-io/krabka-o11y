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
//! makes the blocks live one manifest at a time. [`tsdb_block_sha256`] names
//! that content.

use std::{collections::BTreeMap, sync::Arc};

use arrow::record_batch::RecordBatch;
use krabka_blockstore::{BlockStoreError, BlockWriter, Labels, escape_object_path_segment};
use num_traits::ToPrimitive;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, path::Path};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    BucketSpan, CompactionIndexError, CompactionIndexManifest, CompactionObjectPlan, FloatRow,
    HistogramCodecError, MetricBlockKind, NativeHistogram, NativeHistogramRow, ResetHint,
    TenantBatches, TenantCompactionRows,
    compactor::{compaction_index_key, series_labels_for_kind},
    encode_tenant_batches,
    wire::validate_spans_and_counts,
};

mod bit_reader;
mod byte_reader;
mod checked_section;
mod chunk_encoding;
mod chunk_error;
mod chunk_meta;
mod chunk_samples;
mod chunk_segments;
mod counter_reset_hint;
mod create_import_json;
mod decode_float_histogram_chunk;
mod decode_histogram_chunk;
mod decode_tsdb_block;
mod decode_xor_chunk;
mod decoded_tsdb_block;
mod format_labels;
mod histogram_layout;
mod i64_to_f64;
mod import_commit;
mod index_series;
mod index_toc;
mod publish_tsdb_import;
mod put_import_json;
mod read_import_json;
mod read_series;
mod read_symbols;
mod stale_histogram;
mod tombstones;
mod tsdb_block_files;
mod tsdb_block_meta;
mod tsdb_block_sha256;
mod tsdb_import_binding;
mod tsdb_import_error;
mod tsdb_import_keys;
mod tsdb_import_limits;
mod tsdb_import_object;
mod tsdb_import_outcome;
mod tsdb_import_record;
mod tsdb_import_stats;
mod tsdb_import_target;
mod tsdb_publish_error;
mod u64_to_f64;
mod validate_postings;
mod xor_state;

use self::{
    bit_reader::BitReader, byte_reader::ByteReader, checked_section::checked_section,
    chunk_encoding::ChunkEncoding, chunk_error::ChunkError, chunk_meta::ChunkMeta,
    chunk_samples::ChunkSamples, chunk_segments::ChunkSegments,
    counter_reset_hint::counter_reset_hint, create_import_json::create_import_json,
    decode_float_histogram_chunk::decode_float_histogram_chunk,
    decode_histogram_chunk::decode_histogram_chunk, decode_xor_chunk::decode_xor_chunk,
    format_labels::format_labels, histogram_layout::HistogramLayout, i64_to_f64::i64_to_f64,
    import_commit::ImportCommit, index_series::IndexSeries, index_toc::IndexToc,
    put_import_json::put_import_json, read_import_json::read_import_json, read_series::read_series,
    read_symbols::read_symbols, stale_histogram::stale_histogram, tombstones::Tombstones,
    tsdb_import_keys::TsdbImportKeys, u64_to_f64::u64_to_f64, validate_postings::validate_postings,
    xor_state::XorState,
};
pub use self::{
    decode_tsdb_block::decode_tsdb_block, decoded_tsdb_block::DecodedTsdbBlock,
    publish_tsdb_import::publish_tsdb_import, tsdb_block_files::TsdbBlockFiles,
    tsdb_block_meta::TsdbBlockMeta, tsdb_block_sha256::tsdb_block_sha256,
    tsdb_import_binding::TsdbImportBinding, tsdb_import_error::TsdbImportError,
    tsdb_import_limits::TsdbImportLimits, tsdb_import_object::TsdbImportObject,
    tsdb_import_outcome::TsdbImportOutcome, tsdb_import_record::TsdbImportRecord,
    tsdb_import_stats::TsdbImportStats, tsdb_import_target::TsdbImportTarget,
    tsdb_publish_error::TsdbPublishError,
};

/// The bit pattern Prometheus writes for a stale marker.
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;
