use super::{
    ByteSize, DecodedExemplar, DecodedSample, DecodedSeries, Message, SymbolTable, WireError,
    WrittenCounts, labels_from_refs, metadata_from_v2, pb, snappy_block_decode,
    v2_histogram_to_native,
};
use crate::wire::DecodedHistogramSample;

#[derive(Clone, Debug, PartialEq)]
pub struct DecodedV2Write {
    pub series: Vec<DecodedSeries>,
    pub counts: WrittenCounts,
}

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn decode_v2(body: &[u8], max_decompressed: ByteSize) -> Result<DecodedV2Write, WireError> {
    let raw = snappy_block_decode(body, max_decompressed)?;
    let req = pb::v2::Request::decode(raw.as_slice())
        .map_err(|error| WireError::ProtobufDecode(error.to_string()))?;
    let table = SymbolTable::from_symbols(req.symbols)
        .map_err(|error| WireError::Invalid(error.to_string()))?;

    let mut out = Vec::with_capacity(req.timeseries.len());
    let mut counts = WrittenCounts::default();
    for series in req.timeseries {
        let labels = labels_from_refs(&table, &series.labels_refs)?;
        let metadata = series
            .metadata
            .as_ref()
            .map(|metadata| metadata_from_v2(&table, &labels, metadata))
            .transpose()?;
        let samples = series
            .samples
            .into_iter()
            .map(|sample| {
                DecodedSample::with_start_timestamp(
                    sample.timestamp,
                    sample.value,
                    (sample.start_timestamp != 0).then_some(sample.start_timestamp),
                )
            })
            .collect::<Vec<_>>();
        counts.samples += samples.len() as u64;

        let histograms = series
            .histograms
            .iter()
            .map(|histogram| {
                Ok(DecodedHistogramSample {
                    timestamp_ms: histogram.timestamp,
                    histogram: v2_histogram_to_native(histogram)?,
                })
            })
            .collect::<Result<Vec<_>, WireError>>()?;
        counts.histograms += histograms.len() as u64;

        let exemplars = series
            .exemplars
            .iter()
            .map(|exemplar| {
                Ok(DecodedExemplar {
                    labels: labels_from_refs(&table, &exemplar.labels_refs)?,
                    timestamp_ms: exemplar.timestamp,
                    value: exemplar.value,
                })
            })
            .collect::<Result<Vec<_>, WireError>>()?;
        counts.exemplars += exemplars.len() as u64;

        out.push(DecodedSeries {
            labels,
            samples,
            histograms,
            exemplars,
            metadata,
        });
    }

    Ok(DecodedV2Write {
        series: out,
        counts,
    })
}
