use super::{
    ByteSize, ClockWireError, DecodedClockReading, Message, WireError, decode_reading, pb,
    snappy_block_decode,
};

/// Decodes a snappy-framed [`ClockReadingBatch`](pb::clocks::ClockReadingBatch)
/// into validated readings.
///
/// `max_decompressed` is the same cap the `remote_write` push path applies, so
/// one setting bounds every ingest body this process decompresses.
///
/// # Errors
///
/// Returns an error when the snappy frame declares or produces more than
/// `max_decompressed`, when the protobuf body is malformed, or when any
/// reading fails validation. This function never panics on wire input.
pub fn decode_clock_readings(
    body: &[u8],
    max_decompressed: ByteSize,
) -> Result<Vec<DecodedClockReading>, ClockWireError> {
    let raw = snappy_block_decode(body, max_decompressed)?;
    let batch = pb::clocks::ClockReadingBatch::decode(raw.as_slice())
        .map_err(|error| WireError::ProtobufDecode(error.to_string()))?;

    batch
        .readings
        .into_iter()
        .enumerate()
        .map(|(index, reading)| decode_reading(index, reading))
        .collect()
}
