use super::{BitReader, ChunkError, XorState};

/// Decodes a Prometheus XOR (Gorilla) float chunk.
///
/// The layout is a big-endian `u16` sample count, then the first timestamp as
/// a varint and the first value as 64 raw bits, then the second timestamp as a
/// uvarint delta, then delta-of-delta timestamps. Each value after the first
/// is combined by XOR with the previous one.
pub fn decode_xor_chunk(data: &[u8]) -> Result<Vec<(i64, f64)>, ChunkError> {
    let (header, body) = data.split_at_checked(2).ok_or(ChunkError::Truncated)?;
    let total = u16::from_be_bytes([header[0], header[1]]);
    let mut reader = BitReader::new(body);
    let mut samples = Vec::with_capacity(usize::from(total));
    let mut timestamp = 0_i64;
    let mut delta = 0_u64;
    let mut value = 0_f64;
    let mut state = XorState::default();
    for index in 0..total {
        match index {
            0 => {
                timestamp = reader.read_varint().ok_or(ChunkError::Truncated)?;
                value = f64::from_bits(reader.read_bits(64).ok_or(ChunkError::Truncated)?);
            }
            1 => {
                delta = reader.read_uvarint().ok_or(ChunkError::Truncated)?;
                timestamp = timestamp.wrapping_add(delta.cast_signed());
                reader
                    .read_xor_value(&mut value, &mut state)
                    .ok_or(ChunkError::Truncated)?;
            }
            _ => {
                let dod = reader.read_xor_dod().ok_or(ChunkError::Truncated)?;
                delta = delta.cast_signed().wrapping_add(dod).cast_unsigned();
                timestamp = timestamp.wrapping_add(delta.cast_signed());
                reader
                    .read_xor_value(&mut value, &mut state)
                    .ok_or(ChunkError::Truncated)?;
            }
        }
        samples.push((timestamp, value));
    }
    Ok(samples)
}
