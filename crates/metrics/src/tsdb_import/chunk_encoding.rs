/// The sample encodings that a Prometheus chunk can declare.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChunkEncoding {
    Xor,
    Histogram,
    FloatHistogram,
}

impl ChunkEncoding {
    /// Maps the chunk's encoding byte. Prometheus reserves the other values,
    /// and the import refuses them rather than guessing a layout.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Xor),
            2 => Some(Self::Histogram),
            3 => Some(Self::FloatHistogram),
            _ => None,
        }
    }
}
