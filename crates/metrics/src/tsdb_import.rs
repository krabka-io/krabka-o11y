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

use std::collections::BTreeMap;

use krabka_blockstore::Labels;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::{
    BucketSpan, FloatRow, NativeHistogram, NativeHistogramRow, ResetHint, TenantCompactionRows,
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
mod decode_float_histogram_chunk;
mod decode_histogram_chunk;
mod decode_tsdb_block;
mod decode_xor_chunk;
mod decoded_tsdb_block;
mod format_labels;
mod histogram_layout;
mod i64_to_f64;
mod index_series;
mod index_toc;
mod read_series;
mod read_symbols;
mod stale_histogram;
mod tombstones;
mod tsdb_block_files;
mod tsdb_block_meta;
mod tsdb_import_error;
mod tsdb_import_limits;
mod tsdb_import_stats;
mod u64_to_f64;
mod validate_postings;
mod xor_state;

use self::{
    bit_reader::BitReader, byte_reader::ByteReader, checked_section::checked_section,
    chunk_encoding::ChunkEncoding, chunk_error::ChunkError, chunk_meta::ChunkMeta,
    chunk_samples::ChunkSamples, chunk_segments::ChunkSegments,
    counter_reset_hint::counter_reset_hint,
    decode_float_histogram_chunk::decode_float_histogram_chunk,
    decode_histogram_chunk::decode_histogram_chunk, decode_xor_chunk::decode_xor_chunk,
    format_labels::format_labels, histogram_layout::HistogramLayout, i64_to_f64::i64_to_f64,
    index_series::IndexSeries, index_toc::IndexToc, read_series::read_series,
    read_symbols::read_symbols, stale_histogram::stale_histogram, tombstones::Tombstones,
    u64_to_f64::u64_to_f64, validate_postings::validate_postings, xor_state::XorState,
};
pub use self::{
    decode_tsdb_block::decode_tsdb_block, decoded_tsdb_block::DecodedTsdbBlock,
    tsdb_block_files::TsdbBlockFiles, tsdb_block_meta::TsdbBlockMeta,
    tsdb_import_error::TsdbImportError, tsdb_import_limits::TsdbImportLimits,
    tsdb_import_stats::TsdbImportStats,
};

/// The bit pattern Prometheus writes for a stale marker.
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;
