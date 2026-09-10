use super::span_id_be_bytes_from_u64;

// Jaeger carries a span id as a signed 64-bit integer, and OTLP carries the
// same id as its eight big-endian bytes. The reading is the one the profiles
// side uses too, so it comes from the block store rather than from here.
pub(crate) fn i64_bytes(value: i64) -> [u8; 8] {
    span_id_be_bytes_from_u64(value.cast_unsigned())
}
