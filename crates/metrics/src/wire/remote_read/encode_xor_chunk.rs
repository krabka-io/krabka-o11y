use super::{RemoteReadError, v1};

const MAX_SAMPLES_PER_CHUNK: usize = 120;

pub(crate) fn encode_xor_chunks(samples: &[v1::Sample]) -> Result<Vec<v1::Chunk>, RemoteReadError> {
    let mut samples = samples.to_vec();
    samples.sort_by_key(|sample| sample.timestamp);
    samples
        .chunks(MAX_SAMPLES_PER_CHUNK)
        .map(encode_xor_chunk)
        .collect()
}

fn encode_xor_chunk(samples: &[v1::Sample]) -> Result<v1::Chunk, RemoteReadError> {
    let count = u16::try_from(samples.len()).expect("XOR chunks are capped at 120 samples");
    let mut bits = BitStream::new(count);
    let mut timestamp = i64::MIN;
    let mut value = 0.0;
    let mut timestamp_delta = 0_u64;
    let mut leading = u8::MAX;
    let mut trailing = 0_u8;

    for (index, sample) in samples.iter().enumerate() {
        match index {
            0 => {
                bits.write_varint(sample.timestamp);
                bits.write_bits(sample.value.to_bits(), 64);
            }
            1 => {
                timestamp_delta = u64::try_from(
                    i128::from(sample.timestamp) - i128::from(timestamp),
                )
                .map_err(|_| RemoteReadError::UnorderedSamples {
                    previous: timestamp,
                    current: sample.timestamp,
                })?;
                bits.write_uvarint(timestamp_delta);
                write_value_delta(&mut bits, sample.value, value, &mut leading, &mut trailing);
            }
            _ => {
                let next_delta = u64::try_from(
                    i128::from(sample.timestamp) - i128::from(timestamp),
                )
                .map_err(|_| RemoteReadError::UnorderedSamples {
                    previous: timestamp,
                    current: sample.timestamp,
                })?;
                write_timestamp_delta_of_delta(
                    &mut bits,
                    next_delta.wrapping_sub(timestamp_delta).cast_signed(),
                );
                timestamp_delta = next_delta;
                write_value_delta(&mut bits, sample.value, value, &mut leading, &mut trailing);
            }
        }
        timestamp = sample.timestamp;
        value = sample.value;
    }

    Ok(v1::Chunk {
        min_time_ms: samples.first().map_or(0, |sample| sample.timestamp),
        max_time_ms: samples.last().map_or(0, |sample| sample.timestamp),
        r#type: v1::chunk::Encoding::Xor as i32,
        data: bits.bytes,
    })
}

fn write_timestamp_delta_of_delta(bits: &mut BitStream, delta: i64) {
    if delta == 0 {
        bits.write_bit(false);
    } else if fits_signed(delta, 14) {
        bits.write_bits(0b10, 2);
        bits.write_bits(delta.cast_unsigned(), 14);
    } else if fits_signed(delta, 17) {
        bits.write_bits(0b110, 3);
        bits.write_bits(delta.cast_unsigned(), 17);
    } else if fits_signed(delta, 20) {
        bits.write_bits(0b1110, 4);
        bits.write_bits(delta.cast_unsigned(), 20);
    } else {
        bits.write_bits(0b1111, 4);
        bits.write_bits(delta.cast_unsigned(), 64);
    }
}

fn fits_signed(value: i64, width: u32) -> bool {
    -((1_i64 << (width - 1)) - 1) <= value && value <= 1_i64 << (width - 1)
}

fn write_value_delta(
    bits: &mut BitStream,
    next: f64,
    current: f64,
    leading: &mut u8,
    trailing: &mut u8,
) {
    let delta = next.to_bits() ^ current.to_bits();
    if delta == 0 {
        bits.write_bit(false);
        return;
    }
    bits.write_bit(true);

    let next_leading = u8::try_from(delta.leading_zeros()).unwrap().min(31);
    let next_trailing = u8::try_from(delta.trailing_zeros()).unwrap();
    if *leading != u8::MAX && next_leading >= *leading && next_trailing >= *trailing {
        bits.write_bit(false);
        bits.write_bits(
            delta >> *trailing,
            64 - u32::from(*leading) - u32::from(*trailing),
        );
        return;
    }

    *leading = next_leading;
    *trailing = next_trailing;
    bits.write_bit(true);
    bits.write_bits(u64::from(next_leading), 5);
    let significant = 64 - u32::from(next_leading) - u32::from(next_trailing);
    bits.write_bits(u64::from(significant & 0x3f), 6);
    bits.write_bits(delta >> next_trailing, significant);
}

struct BitStream {
    bytes: Vec<u8>,
    available: u8,
}

impl BitStream {
    fn new(sample_count: u16) -> Self {
        Self {
            bytes: sample_count.to_be_bytes().to_vec(),
            available: 0,
        }
    }

    fn write_bit(&mut self, value: bool) {
        if self.available == 0 {
            self.bytes.push(0);
            self.available = 8;
        }
        if value {
            let last = self
                .bytes
                .last_mut()
                .expect("bit stream has a current byte");
            *last |= 1 << (self.available - 1);
        }
        self.available -= 1;
    }

    fn write_byte(&mut self, value: u8) {
        if self.available == 0 {
            self.bytes.push(value);
            return;
        }
        let last = self
            .bytes
            .last_mut()
            .expect("bit stream has a current byte");
        *last |= value >> (8 - self.available);
        self.bytes.push(value << self.available);
    }

    fn write_bits(&mut self, value: u64, width: u32) {
        let shifted = if width == 64 {
            value
        } else {
            value << (64 - width)
        };
        let mut remaining = width;
        let mut shifted = shifted;
        while remaining >= 8 {
            self.write_byte((shifted >> 56) as u8);
            shifted <<= 8;
            remaining -= 8;
        }
        while remaining > 0 {
            self.write_bit(shifted >> 63 == 1);
            shifted <<= 1;
            remaining -= 1;
        }
    }

    fn write_uvarint(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.write_byte(
                u8::try_from(value & 0x7f).expect("masked uvarint byte fits in u8") | 0x80,
            );
            value >>= 7;
        }
        self.write_byte(u8::try_from(value).expect("terminal uvarint byte fits in u8"));
    }

    fn write_varint(&mut self, value: i64) {
        let mut encoded = value.cast_unsigned() << 1;
        if value < 0 {
            encoded = !encoded;
        }
        self.write_uvarint(encoded);
    }
}
