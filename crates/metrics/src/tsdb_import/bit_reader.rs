use super::XorState;

/// A most-significant-bit-first reader over one chunk's data, the order in
/// which Prometheus writes its `bstream`.
///
/// Every read returns `None` at the end of the data rather than panicking, so
/// a truncated chunk surfaces as a decode error.
pub struct BitReader<'a> {
    data: &'a [u8],
    /// The number of bits already consumed.
    position: usize,
}

impl<'a> BitReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    pub fn read_bit(&mut self) -> Option<bool> {
        let byte = *self.data.get(self.position / 8)?;
        let bit = (byte >> (7 - (self.position % 8))) & 1 == 1;
        self.position += 1;
        Some(bit)
    }

    /// Reads `count` bits, at most 64, as the low bits of the result.
    pub fn read_bits(&mut self, count: u8) -> Option<u64> {
        if count > 64 || self.data.len() * 8 - self.position < usize::from(count) {
            return None;
        }
        let mut value = 0_u64;
        for _ in 0..count {
            value = (value << 1) | u64::from(self.read_bit()?);
        }
        Some(value)
    }

    pub fn read_byte(&mut self) -> Option<u8> {
        self.read_bits(8).and_then(|bits| u8::try_from(bits).ok())
    }

    /// Reads a Go `binary.ReadUvarint` value, which may start mid-byte.
    pub fn read_uvarint(&mut self) -> Option<u64> {
        let mut value = 0_u64;
        for index in 0..10 {
            let byte = self.read_byte()?;
            if index == 9 && byte > 1 {
                return None;
            }
            value |= u64::from(byte & 0x7f) << (7 * index);
            if byte & 0x80 == 0 {
                return Some(value);
            }
        }
        None
    }

    /// Reads a Go `binary.ReadVarint` value: a zig-zag encoded uvarint.
    pub fn read_varint(&mut self) -> Option<i64> {
        let raw = self.read_uvarint()?;
        let magnitude = (raw >> 1).cast_signed();
        Some(if raw & 1 == 0 { magnitude } else { !magnitude })
    }

    /// Reads a Prometheus `varbit` integer: a unary prefix picks the width,
    /// and the value is two's complement within that width.
    pub fn read_varbit_int(&mut self) -> Option<i64> {
        let (width, raw) = self.read_varbit_raw()?;
        if width == 64 || width == 0 {
            return Some(raw.cast_signed());
        }
        let signed = if raw > 1 << (width - 1) {
            raw.wrapping_sub(1 << width)
        } else {
            raw
        };
        Some(signed.cast_signed())
    }

    /// Reads a Prometheus unsigned `varbit` value.
    pub fn read_varbit_uint(&mut self) -> Option<u64> {
        self.read_varbit_raw().map(|(_, raw)| raw)
    }

    fn read_varbit_raw(&mut self) -> Option<(u8, u64)> {
        let mut ones = 0_u8;
        while ones < 8 && self.read_bit()? {
            ones += 1;
        }
        let width = match ones {
            0 => return Some((0, 0)),
            1 => 3,
            2 => 6,
            3 => 9,
            4 => 12,
            5 => 18,
            6 => 25,
            7 => 56,
            _ => 64,
        };
        Some((width, self.read_bits(width)?))
    }

    /// Reads a timestamp delta-of-delta in the XOR chunk encoding.
    pub fn read_xor_dod(&mut self) -> Option<i64> {
        let mut ones = 0_u8;
        while ones < 4 && self.read_bit()? {
            ones += 1;
        }
        let width = match ones {
            0 => return Some(0),
            1 => 14,
            2 => 17,
            3 => 20,
            _ => return self.read_bits(64).map(u64::cast_signed),
        };
        let raw = self.read_bits(width)?;
        let signed = if raw > 1 << (width - 1) {
            raw.wrapping_sub(1 << width)
        } else {
            raw
        };
        Some(signed.cast_signed())
    }

    /// Applies one Gorilla XOR value update to `value`.
    ///
    /// `state` holds the leading and trailing zero counts that the previous
    /// value with new counts stored. A control block that would place the
    /// significant bits outside the 64-bit word returns `None`.
    pub fn read_xor_value(&mut self, value: &mut f64, state: &mut XorState) -> Option<()> {
        if !self.read_bit()? {
            return Some(());
        }
        let (leading, significant) = if self.read_bit()? {
            let leading = u8::try_from(self.read_bits(5)?).ok()?;
            let significant = match u8::try_from(self.read_bits(6)?).ok()? {
                0 => 64,
                bits => bits,
            };
            let trailing = 64_u8.checked_sub(leading)?.checked_sub(significant)?;
            *state = XorState { leading, trailing };
            (leading, significant)
        } else {
            let significant = 64_u8
                .checked_sub(state.leading)?
                .checked_sub(state.trailing)?;
            (state.leading, significant)
        };
        let bits = self.read_bits(significant)?;
        let trailing = 64 - leading - significant;
        let shifted = bits.checked_shl(u32::from(trailing)).unwrap_or(0);
        *value = f64::from_bits(value.to_bits() ^ shifted);
        Some(())
    }
}
