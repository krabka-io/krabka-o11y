use super::*;

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
        rows: TenantCompactionRows::empty(tenant),
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
        let labels = Labels::from_pairs(entry.labels.iter().cloned());
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
                    .map(|sample| sample.timestamp_ms)
                    .collect::<Vec<_>>(),
                ChunkSamples::Histograms(samples) => {
                    samples.iter().map(|sample| sample.timestamp_ms).collect()
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
        let chunk_data = self.chunks.chunk(reference)?;
        let encoding = chunk_data.encoding;
        let data = chunk_data.data;
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
                for sample in samples {
                    let timestamp_ms = sample.timestamp_ms;
                    if self.tombstones.deletes(series, timestamp_ms) {
                        self.stats.deleted_samples += 1;
                        continue;
                    }
                    self.push_float(fingerprint, timestamp_ms, sample.value);
                    kept += 1;
                }
            }
            ChunkSamples::Histograms(samples) => {
                for sample in samples {
                    let timestamp_ms = sample.timestamp_ms;
                    let hist = sample.histogram;
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

/// Formats label pairs as a Prometheus series selector for error messages.
pub fn format_labels(labels: &[super::MetricLabelPair<super::MetricString>]) -> String {
    let pairs = labels
        .iter()
        .map(|pair| {
            let quoted = pair
                .value
                .utf8()
                .map_or_else(|| pair.value.quoted(), |value| format!("{value:?}"));
            format!("{}={quoted}", pair.name)
        })
        .collect::<Vec<_>>();
    format!("{{{}}}", pairs.join(", "))
}

/// One series entry of a TSDB index.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexSeries {
    /// The series reference: the entry's byte offset divided by 16. Postings
    /// and tombstones name a series by it.
    pub reference: u64,
    /// The label pairs, sorted by name.
    pub labels: Vec<MetricLabelPair<MetricString>>,
    pub chunks: Vec<ChunkMeta>,
}

/// The table of contents at the end of a TSDB index file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexToc {
    pub symbols: u64,
    pub series: u64,
    pub label_indices: u64,
    pub label_indices_table: u64,
    pub postings: u64,
    pub postings_table: u64,
}

/// Six big-endian `u64` offsets and a CRC32C over them.
pub const INDEX_TOC_LEN: usize = 52;
const HEADER_LEN: u64 = 5;

impl IndexToc {
    /// Reads and checks the TOC. The sections must appear in the order that
    /// the Prometheus writer uses, and all of them must start after the file
    /// header and before the TOC.
    pub fn read(index: &[u8]) -> Result<Self, TsdbImportError> {
        let start = index
            .len()
            .checked_sub(INDEX_TOC_LEN)
            .ok_or(TsdbImportError::Truncated {
                section: "index TOC",
            })?;
        let toc = &index[start..];
        let expected = u32::from_be_bytes([toc[48], toc[49], toc[50], toc[51]]);
        if crc32c::crc32c(&toc[..48]) != expected {
            return Err(TsdbImportError::Checksum {
                section: "index TOC".to_owned(),
            });
        }
        let mut reader = ByteReader::new(&toc[..48], "index TOC");
        let parsed = Self {
            symbols: reader.be64()?,
            series: reader.be64()?,
            label_indices: reader.be64()?,
            label_indices_table: reader.be64()?,
            postings: reader.be64()?,
            postings_table: reader.be64()?,
        };
        let end = u64::try_from(start).unwrap_or(u64::MAX);
        let ordered = [
            HEADER_LEN,
            parsed.symbols,
            parsed.series,
            parsed.label_indices,
            parsed.postings,
            parsed.label_indices_table,
            parsed.postings_table,
            end,
        ];
        if ordered.windows(2).any(|pair| pair[0] > pair[1]) || parsed.postings_table == end {
            return Err(TsdbImportError::InvalidIndex(format!(
                "table of contents sections are out of order: {parsed:?}"
            )));
        }
        Ok(parsed)
    }
}

/// Reads every series entry between the series and label-index offsets of
/// the TOC.
///
/// Each entry starts on a 16-byte boundary, which gives the series its
/// reference. The section offset in the TOC may point before the first
/// boundary. The bytes between entries must be zero. The entry holds its
/// label symbol references and chunk metas, and a CRC32C over both. The
/// checks match the invariants that the Prometheus index writer enforces:
/// series strictly sorted by labels, label names strictly sorted within a
/// series, chunk references non-decreasing, and chunk time ranges ordered and
/// disjoint.
///
/// The chunk count of each series goes into a block-wide total before the
/// function reads the chunks of that series. A block over
/// [`TsdbImportLimits::max_chunks`] therefore stops the read before the kept
/// chunk lists grow past that bound.
pub fn read_series(
    index: &[u8],
    toc: &IndexToc,
    symbols: &[MetricString],
    limits: &TsdbImportLimits,
) -> Result<Vec<IndexSeries>, TsdbImportError> {
    const SECTION: &str = "index series";
    let start =
        usize::try_from(toc.series).map_err(|_| TsdbImportError::Truncated { section: SECTION })?;
    let end = usize::try_from(toc.label_indices)
        .map_err(|_| TsdbImportError::Truncated { section: SECTION })?;
    let region = index
        .get(..end)
        .ok_or(TsdbImportError::Truncated { section: SECTION })?;
    let mut series = Vec::<IndexSeries>::new();
    let mut position = start;
    let mut cursor = ChunkCursor::default();
    while position < end {
        let aligned = position.next_multiple_of(16).min(end);
        if region[position..aligned].iter().any(|byte| *byte != 0) {
            return Err(TsdbImportError::InvalidIndex(format!(
                "series padding at {position} is not zero"
            )));
        }
        position = aligned;
        if position == end {
            break;
        }
        let count = u64::try_from(series.len()).unwrap_or(u64::MAX) + 1;
        TsdbImportLimits::check("series", count, limits.max_series)?;
        let mut reader = ByteReader::new(&region[position..], SECTION);
        let content = reader.uvarint_bytes()?;
        let expected = reader.be32()?;
        let reference = u64::try_from(position / 16).unwrap_or(u64::MAX);
        if crc32c::crc32c(content) != expected {
            return Err(TsdbImportError::Checksum {
                section: format!("index series {reference}"),
            });
        }
        let entry = parse_entry(content, reference, symbols, limits, &mut cursor)?;
        if series
            .last()
            .is_some_and(|last| last.labels >= entry.labels)
        {
            return Err(TsdbImportError::SeriesOrder {
                labels: format_labels(&entry.labels),
            });
        }
        series.push(entry);
        position += reader.position();
    }
    Ok(series)
}

/// The chunk state that one series entry carries to the next.
#[derive(Default)]
struct ChunkCursor {
    /// The reference of the last chunk read, in any series.
    last_reference: u64,
    /// The chunks that the entries read so far declare.
    total: u64,
}

fn parse_entry(
    content: &[u8],
    reference: u64,
    symbols: &[MetricString],
    limits: &TsdbImportLimits,
    cursor: &mut ChunkCursor,
) -> Result<IndexSeries, TsdbImportError> {
    let mut reader = ByteReader::new(content, "index series entry");
    let label_count = reader.uvarint()?;
    TsdbImportLimits::check(
        "labels per series",
        label_count,
        limits.max_labels_per_series,
    )?;
    let label_count = usize::try_from(label_count).unwrap_or(usize::MAX);
    let mut labels = Vec::<super::MetricLabelPair<MetricString>>::with_capacity(label_count);
    for _ in 0..label_count {
        let name = symbol(symbols, reader.uvarint()?)?.utf8().ok_or_else(|| {
            TsdbImportError::InvalidLabels(format!(
                "series {reference} has a label name that is not valid UTF-8"
            ))
        })?;
        let value = symbol(symbols, reader.uvarint()?)?;
        if name.is_empty() || value.is_empty() {
            return Err(TsdbImportError::InvalidLabels(format!(
                "series {reference} has an empty label name or value"
            )));
        }
        if labels.last().is_some_and(|last| last.name.as_str() >= name) {
            return Err(TsdbImportError::InvalidLabels(format!(
                "series {reference} has label {name:?} out of order or repeated"
            )));
        }
        labels.push(super::MetricLabelPair::new(
            name.to_owned(),
            value.to_owned(),
        ));
    }
    if labels.is_empty() {
        return Err(TsdbImportError::InvalidLabels(format!(
            "series {reference} has no labels"
        )));
    }
    let chunk_count = reader.uvarint()?;
    TsdbImportLimits::check(
        "chunks per series",
        chunk_count,
        limits.max_chunks_per_series,
    )?;
    cursor.total = cursor.total.saturating_add(chunk_count);
    TsdbImportLimits::check("chunks", cursor.total, limits.max_chunks)?;
    let chunk_count = usize::try_from(chunk_count).unwrap_or(usize::MAX);
    let mut chunks = Vec::<ChunkMeta>::with_capacity(chunk_count.min(reader.remaining()));
    let invalid = |reason: String| TsdbImportError::InvalidChunk {
        labels: format_labels(&labels),
        reason,
    };
    for index in 0..chunk_count {
        let chunk = if let Some(previous) = chunks.last() {
            let gap = reader.uvarint()?.cast_signed();
            let min_time = previous
                .max_time
                .checked_add(gap)
                .ok_or_else(|| invalid("chunk time overflows".to_owned()))?;
            let span = reader.uvarint()?.cast_signed();
            let max_time = min_time
                .checked_add(span)
                .ok_or_else(|| invalid("chunk time overflows".to_owned()))?;
            let delta = reader.varint()?;
            let chunk_reference = previous
                .reference
                .cast_signed()
                .checked_add(delta)
                .ok_or_else(|| invalid("chunk reference overflows".to_owned()))?
                .cast_unsigned();
            if min_time <= previous.max_time {
                return Err(invalid(format!(
                    "chunk {index} starts at {min_time}, not after the previous chunk's end {}",
                    previous.max_time
                )));
            }
            ChunkMeta {
                min_time,
                max_time,
                reference: chunk_reference,
            }
        } else {
            let min_time = reader.varint()?;
            let span = reader.uvarint()?.cast_signed();
            let max_time = min_time
                .checked_add(span)
                .ok_or_else(|| invalid("chunk time overflows".to_owned()))?;
            ChunkMeta {
                min_time,
                max_time,
                reference: reader.uvarint()?,
            }
        };
        if chunk.max_time < chunk.min_time {
            return Err(invalid(format!("chunk {index} ends before it starts")));
        }
        if chunk.reference < cursor.last_reference {
            return Err(invalid(format!(
                "chunk reference {:#x} is below the previous reference {:#x}",
                chunk.reference, cursor.last_reference
            )));
        }
        cursor.last_reference = chunk.reference;
        chunks.push(chunk);
    }
    if reader.remaining() != 0 {
        return Err(invalid(
            "series entry holds bytes after its chunks".to_owned(),
        ));
    }
    Ok(IndexSeries {
        reference,
        labels,
        chunks,
    })
}

fn symbol(symbols: &[MetricString], reference: u64) -> Result<&MetricString, TsdbImportError> {
    usize::try_from(reference)
        .ok()
        .and_then(|reference| symbols.get(reference))
        .ok_or_else(|| {
            TsdbImportError::InvalidIndex(format!("symbol reference {reference} is out of range"))
        })
}

/// Reads the symbol table. Symbols retain arbitrary bytes, and the table
/// must be strictly sorted by bytes, as the Prometheus writer produces it.
pub fn read_symbols(
    index: &[u8],
    toc: &IndexToc,
    limits: &TsdbImportLimits,
) -> Result<Vec<MetricString>, TsdbImportError> {
    let content = checked_section(index, toc.symbols, "index symbols")?;
    let mut reader = ByteReader::new(content, "index symbols");
    let count = u64::from(reader.be32()?);
    TsdbImportLimits::check("symbols", count, limits.max_symbols)?;
    let count = usize::try_from(count)
        .ok()
        .filter(|count| *count <= reader.remaining())
        .ok_or(TsdbImportError::Truncated {
            section: "index symbols",
        })?;
    let mut symbols = Vec::<MetricString>::with_capacity(count);
    for _ in 0..count {
        let symbol = MetricString::from(reader.uvarint_bytes()?.to_vec());
        if symbols.last().is_some_and(|last| last >= &symbol) {
            return Err(TsdbImportError::InvalidIndex(format!(
                "symbol {} is out of order",
                symbol.quoted()
            )));
        }
        symbols.push(symbol);
    }
    if reader.remaining() != 0 {
        return Err(TsdbImportError::InvalidIndex(
            "symbol table holds bytes after its last symbol".to_owned(),
        ));
    }
    Ok(symbols)
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PostingLabelIdentity<'a> {
    name: &'a str,
    value: &'a [u8],
}

/// Checks the postings offset table and every postings list against the
/// series section.
///
/// The import reads series from the series section alone, but Prometheus
/// answers selectors from the postings. A block whose postings disagree with
/// its series would query differently in Prometheus and in Krabka, so the
/// import refuses it. The table must list each label pair once, in sorted
/// order, starting with the all-series list under the empty pair, and each
/// list must name exactly the series that carry the pair.
pub fn validate_postings(
    index: &[u8],
    toc: &IndexToc,
    series: &[IndexSeries],
) -> Result<(), TsdbImportError> {
    let mut expected = BTreeMap::<PostingLabelIdentity<'_>, Vec<u32>>::new();
    for entry in series {
        let reference = u32::try_from(entry.reference).map_err(|_| {
            TsdbImportError::InvalidIndex(format!(
                "series reference {} overflows 32 bits",
                entry.reference
            ))
        })?;
        expected
            .entry(PostingLabelIdentity {
                name: "",
                value: b"",
            })
            .or_default()
            .push(reference);
        for label in &entry.labels {
            expected
                .entry(PostingLabelIdentity {
                    name: label.name.as_str(),
                    value: label.value.as_bytes(),
                })
                .or_default()
                .push(reference);
        }
    }

    let table = checked_section(index, toc.postings_table, "index postings offset table")?;
    let mut reader = ByteReader::new(table, "index postings offset table");
    let count = usize::try_from(reader.be32()?).unwrap_or(usize::MAX);
    if count != expected.len() {
        return Err(TsdbImportError::InvalidIndex(format!(
            "postings offset table lists {count} label pairs, series carry {}",
            expected.len()
        )));
    }
    for posting in &expected {
        let identity = posting.0;
        let references = posting.1;
        if reader.uvarint()? != 2 {
            return Err(TsdbImportError::InvalidIndex(
                "postings offset table entry does not hold a label pair".to_owned(),
            ));
        }
        let found_name = reader.uvarint_bytes()?;
        let found_value = reader.uvarint_bytes()?;
        if found_name != identity.name.as_bytes() || found_value != identity.value {
            return Err(TsdbImportError::InvalidIndex(format!(
                "postings offset table does not list {}={:?} in order",
                identity.name, identity.value
            )));
        }
        let offset = reader.uvarint()?;
        check_list(
            index,
            toc,
            offset,
            identity.name,
            identity.value,
            references,
        )?;
    }
    if reader.remaining() != 0 {
        return Err(TsdbImportError::InvalidIndex(
            "postings offset table holds bytes after its last entry".to_owned(),
        ));
    }
    Ok(())
}

fn check_list(
    index: &[u8],
    toc: &IndexToc,
    offset: u64,
    name: &str,
    value: &[u8],
    references: &[u32],
) -> Result<(), TsdbImportError> {
    if offset < toc.postings || offset >= toc.label_indices_table || !offset.is_multiple_of(4) {
        return Err(TsdbImportError::InvalidIndex(format!(
            "postings list for {name}={value:?} starts at {offset}, outside the postings section"
        )));
    }
    let end = usize::try_from(toc.label_indices_table).unwrap_or(usize::MAX);
    let start = usize::try_from(offset).unwrap_or(usize::MAX);
    let section = index.get(start..end).ok_or(TsdbImportError::Truncated {
        section: "index postings",
    })?;
    let mut reader = ByteReader::new(section, "index postings");
    let length = usize::try_from(reader.be32()?).unwrap_or(usize::MAX);
    let content = reader.bytes(length)?;
    let crc = reader.be32()?;
    if crc32c::crc32c(content) != crc {
        return Err(TsdbImportError::Checksum {
            section: format!("postings list for {name}={value:?}"),
        });
    }
    let mut list = ByteReader::new(content, "index postings");
    let count = usize::try_from(list.be32()?).unwrap_or(usize::MAX);
    if count != references.len() || list.remaining() != count.saturating_mul(4) {
        return Err(TsdbImportError::InvalidIndex(format!(
            "postings list for {name}={value:?} does not match the series that carry it"
        )));
    }
    for reference in references {
        if list.be32()? != *reference {
            return Err(TsdbImportError::InvalidIndex(format!(
                "postings list for {name}={value:?} does not match the series that carry it"
            )));
        }
    }
    Ok(())
}
