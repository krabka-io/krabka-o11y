use super::{
    BTreeMap, ChunkEncoding, ChunkSamples, ChunkSegments, DecodedTsdbBlock, FloatRow, IndexSeries,
    IndexToc, Labels, NativeHistogram, NativeHistogramRow, STALE_NAN_BITS, TenantCompactionRows,
    Tombstones, TsdbBlockFiles, TsdbBlockMeta, TsdbImportError, TsdbImportLimits, TsdbImportStats,
    decode_float_histogram_chunk, decode_histogram_chunk, decode_xor_chunk, format_labels,
    read_series, read_symbols, validate_postings, validate_spans_and_counts,
};

const INDEX_MAGIC: u32 = 0xBAAA_D700;

/// The external labels that name the tenant a block belongs to: the Mimir and
/// Cortex label, and the Krabka one.
const TENANT_LABELS: [&str; 2] = ["__org_id__", "__tenant_id__"];

/// Decodes and validates a whole Prometheus TSDB block for `tenant`.
///
/// The decoder reads index format versions 2 and 3, chunk segment format 1,
/// and tombstones format 1. It verifies every checksum, the postings against
/// the series, the ordering rules that the Prometheus writer enforces, the
/// chunk and sample time bounds, and the tenant external label, before it
/// returns anything. Deleted samples are dropped. A stale marker in a
/// histogram chunk becomes a float stale marker for the same series and time,
/// which the query path treats the way Prometheus treats the histogram one.
///
/// # Errors
///
/// Returns the first [`TsdbImportError`] found. The function writes nothing,
/// so an error leaves no partial state.
pub fn decode_tsdb_block(
    tenant: &str,
    meta: &TsdbBlockMeta,
    files: TsdbBlockFiles<'_>,
    limits: &TsdbImportLimits,
) -> Result<DecodedTsdbBlock, TsdbImportError> {
    check_meta(tenant, meta)?;
    let total_bytes = files
        .chunk_segments
        .iter()
        .map(|segment| segment.len())
        .chain([files.index.len(), files.tombstones.map_or(0, <[u8]>::len)])
        .fold(0_u64, |total, length| {
            total.saturating_add(u64::try_from(length).unwrap_or(u64::MAX))
        });
    TsdbImportLimits::check("block bytes", total_bytes, limits.max_block_bytes)?;

    let series = read_index(files.index, limits)?;
    let chunks = ChunkSegments::new(files.chunk_segments.to_vec())?;
    let tombstones = files.tombstones.map_or_else(
        || Ok(Tombstones::default()),
        |file| Tombstones::parse(file, limits.max_series.saturating_mul(16)),
    )?;

    if let Some(unknown) = tombstones.series().find(|reference| {
        series
            .binary_search_by_key(reference, |entry| entry.reference)
            .is_err()
    }) {
        return Err(TsdbImportError::InvalidIndex(format!(
            "tombstones name series reference {unknown:#x}, which the index does not hold"
        )));
    }

    let mut converter = Converter {
        meta,
        limits,
        chunks: &chunks,
        tombstones: &tombstones,
        rows: TenantCompactionRows {
            tenant: tenant.to_owned(),
            series_labels: BTreeMap::new(),
            float_rows: Vec::new(),
            histogram_rows: Vec::new(),
            exemplar_rows: Vec::new(),
            metadata_rows: Vec::new(),
            clock_rows: Vec::new(),
        },
        stats: TsdbImportStats::default(),
        decoded_samples: 0,
    };
    for entry in &series {
        converter.convert_series(entry)?;
    }
    let Converter {
        mut rows, stats, ..
    } = converter;
    if rows.float_rows.is_empty() && rows.histogram_rows.is_empty() {
        return Err(TsdbImportError::Empty);
    }
    rows.float_rows
        .sort_by_key(|row| (row.fingerprint, row.timestamp_ms));
    rows.histogram_rows
        .sort_by_key(|row| (row.fingerprint, row.timestamp_ms));
    Ok(DecodedTsdbBlock { rows, stats })
}

fn check_meta(tenant: &str, meta: &TsdbBlockMeta) -> Result<(), TsdbImportError> {
    if meta.min_time < 0 || meta.max_time <= meta.min_time {
        return Err(TsdbImportError::InvalidMeta(format!(
            "minTime {} and maxTime {} do not form a range",
            meta.min_time, meta.max_time
        )));
    }
    for label in TENANT_LABELS {
        if let Some(found) = meta.external_labels.get(label)
            && found != tenant
        {
            return Err(TsdbImportError::TenantMismatch {
                label: label.to_owned(),
                expected: tenant.to_owned(),
                found: found.clone(),
            });
        }
    }
    Ok(())
}

fn read_index(
    index: &[u8],
    limits: &TsdbImportLimits,
) -> Result<Vec<IndexSeries>, TsdbImportError> {
    let header = index.get(..5).ok_or(TsdbImportError::Truncated {
        section: "index header",
    })?;
    let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
    if magic != INDEX_MAGIC {
        return Err(TsdbImportError::BadMagic {
            file: "index",
            found: magic,
            expected: INDEX_MAGIC,
        });
    }
    if !matches!(header[4], 2 | 3) {
        return Err(TsdbImportError::UnsupportedIndexVersion(header[4]));
    }
    let toc = IndexToc::read(index)?;
    let symbols = read_symbols(index, &toc, limits)?;
    let series = read_series(index, &toc, &symbols, limits)?;
    validate_postings(index, &toc, &series)?;
    Ok(series)
}

struct Converter<'a> {
    meta: &'a TsdbBlockMeta,
    limits: &'a TsdbImportLimits,
    chunks: &'a ChunkSegments<'a>,
    tombstones: &'a Tombstones,
    rows: TenantCompactionRows,
    stats: TsdbImportStats,
    decoded_samples: u64,
}

impl Converter<'_> {
    fn convert_series(&mut self, entry: &IndexSeries) -> Result<(), TsdbImportError> {
        let labels = entry.labels.iter().cloned().collect::<Labels>();
        let fingerprint = labels.fingerprint();
        let display = format_labels(&entry.labels);
        if self.rows.series_labels.contains_key(&fingerprint) {
            return Err(TsdbImportError::InvalidLabels(format!(
                "series {display} has the same fingerprint as another series"
            )));
        }
        let mut previous = None::<i64>;
        let mut kept = 0_u64;
        for chunk in &entry.chunks {
            self.check_bounds(&display, chunk.min_time)?;
            self.check_bounds(&display, chunk.max_time)?;
            let samples = self.decode_chunk(&display, chunk.reference)?;
            let timestamps = match &samples {
                ChunkSamples::Floats(samples) => samples
                    .iter()
                    .map(|(timestamp, _)| *timestamp)
                    .collect::<Vec<_>>(),
                ChunkSamples::Histograms(samples) => {
                    samples.iter().map(|(timestamp, _)| *timestamp).collect()
                }
            };
            if timestamps.is_empty() {
                return Err(TsdbImportError::InvalidChunk {
                    labels: display,
                    reason: format!("chunk {:#x} holds no samples", chunk.reference),
                });
            }
            for timestamp in &timestamps {
                if !(chunk.min_time..=chunk.max_time).contains(timestamp) {
                    return Err(TsdbImportError::InvalidChunk {
                        labels: display,
                        reason: format!(
                            "sample {timestamp} is outside its chunk range [{}, {}]",
                            chunk.min_time, chunk.max_time
                        ),
                    });
                }
                if let Some(previous) = previous
                    && *timestamp <= previous
                {
                    return Err(TsdbImportError::SampleOrder {
                        labels: display,
                        previous,
                        timestamp: *timestamp,
                    });
                }
                previous = Some(*timestamp);
            }
            self.stats.chunks += 1;
            kept += self.push_samples(entry.reference, fingerprint, samples)?;
        }
        if kept > 0 {
            self.stats.series += 1;
            self.rows.series_labels.insert(fingerprint, labels);
        }
        Ok(())
    }

    fn check_bounds(&self, display: &str, timestamp: i64) -> Result<(), TsdbImportError> {
        if timestamp < self.meta.min_time || timestamp >= self.meta.max_time {
            return Err(TsdbImportError::OutOfBounds {
                labels: display.to_owned(),
                timestamp,
                min_time: self.meta.min_time,
                max_time: self.meta.max_time,
            });
        }
        Ok(())
    }

    fn decode_chunk(
        &mut self,
        display: &str,
        reference: u64,
    ) -> Result<ChunkSamples, TsdbImportError> {
        let (encoding, data) = self.chunks.chunk(reference)?;
        let declared = data.get(..2).map_or(0, |header| {
            u64::from(u16::from_be_bytes([header[0], header[1]]))
        });
        self.decoded_samples = self.decoded_samples.saturating_add(declared);
        TsdbImportLimits::check("samples", self.decoded_samples, self.limits.max_samples)?;
        let max_buckets = self.limits.max_histogram_buckets;
        let decoded = match encoding {
            ChunkEncoding::Xor => decode_xor_chunk(data).map(ChunkSamples::Floats),
            ChunkEncoding::Histogram => {
                decode_histogram_chunk(data, max_buckets).map(ChunkSamples::Histograms)
            }
            ChunkEncoding::FloatHistogram => {
                decode_float_histogram_chunk(data, max_buckets).map(ChunkSamples::Histograms)
            }
        };
        decoded.map_err(|error| match error {
            super::ChunkError::Schema(schema) => {
                TsdbImportError::UnsupportedHistogramSchema(schema)
            }
            super::ChunkError::TooManyBuckets(count) => TsdbImportError::LimitExceeded {
                limit: "histogram buckets",
                value: count,
                max: max_buckets,
            },
            other => TsdbImportError::InvalidChunk {
                labels: display.to_owned(),
                reason: format!("chunk {reference:#x}: {other}"),
            },
        })
    }

    fn push_samples(
        &mut self,
        series: u64,
        fingerprint: u64,
        samples: ChunkSamples,
    ) -> Result<u64, TsdbImportError> {
        let mut kept = 0;
        match samples {
            ChunkSamples::Floats(samples) => {
                for (timestamp_ms, value) in samples {
                    if self.tombstones.deletes(series, timestamp_ms) {
                        self.stats.deleted_samples += 1;
                        continue;
                    }
                    self.push_float(fingerprint, timestamp_ms, value);
                    kept += 1;
                }
            }
            ChunkSamples::Histograms(samples) => {
                for (timestamp_ms, hist) in samples {
                    if self.tombstones.deletes(series, timestamp_ms) {
                        self.stats.deleted_samples += 1;
                        continue;
                    }
                    kept += 1;
                    if hist.sum.to_bits() == STALE_NAN_BITS {
                        self.stats.stale_histogram_markers += 1;
                        self.push_float(fingerprint, timestamp_ms, hist.sum);
                        continue;
                    }
                    validate_histogram(&hist)?;
                    self.stats.histogram_samples += 1;
                    self.rows.histogram_rows.push(NativeHistogramRow {
                        fingerprint,
                        timestamp_ms,
                        hist,
                    });
                }
            }
        }
        Ok(kept)
    }

    fn push_float(&mut self, fingerprint: u64, timestamp_ms: i64, value: f64) {
        self.stats.float_samples += 1;
        self.rows.float_rows.push(FloatRow {
            fingerprint,
            timestamp_ms,
            value,
            start_timestamp_ms: None,
        });
    }
}

fn validate_histogram(hist: &NativeHistogram) -> Result<(), TsdbImportError> {
    validate_spans_and_counts(
        hist.schema,
        &hist.positive_spans,
        &hist.positive_counts,
        &hist.negative_spans,
        &hist.negative_counts,
        hist.custom_values.as_deref(),
    )
    .map_err(|error| TsdbImportError::InvalidHistogram(error.to_string()))?;
    if hist.zero_threshold < 0.0 || hist.zero_threshold.is_nan() {
        return Err(TsdbImportError::InvalidHistogram(format!(
            "zero threshold {} is not a non-negative number",
            hist.zero_threshold
        )));
    }
    if let Some(bounds) = &hist.custom_values
        && bounds
            .windows(2)
            .any(|pair| pair[0].partial_cmp(&pair[1]) != Some(std::cmp::Ordering::Less))
    {
        return Err(TsdbImportError::InvalidHistogram(
            "custom bucket bounds are not strictly increasing".to_owned(),
        ));
    }
    Ok(())
}
