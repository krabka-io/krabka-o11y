//! `clap` value parsers that several service binaries share.
//!
//! Each function has the shape `fn(&str) -> Result<T, String>` that a
//! `#[arg(value_parser = ...)]` attribute expects, so a binary names it
//! directly and `--help`, the environment and the config file all go through
//! the same check.

use krabka_client_consumer::ConsumerFetchMaxBytes;
use krabka_client_core::{ClientFrameMax, ConnectionDispatchQueueCapacity};
use krabka_units::{ByteSize, convert::ByteSizeExt as _, parse};

/// The largest whole-byte count that a `UOM` `f64` byte size holds exactly,
/// which is 2^53.
const MAX_EXACT_WHOLE_BYTES: f64 = 9_007_199_254_740_992.0;

/// Parses a count that must be at least one.
///
/// # Errors
///
/// Returns the parse error for text that is not a `usize`, and an error
/// naming the received value for `0`.
pub fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let count = value.parse::<usize>().map_err(|error| error.to_string())?;
    if count == 0 {
        return Err(format!(
            "the value must be greater than 0, but received {count}"
        ));
    }
    Ok(count)
}

/// Parses a `u32` count that must be at least one.
///
/// # Errors
///
/// Returns the parse error for text that is not a `u32`, and an error for `0`.
pub fn parse_positive_u32(value: &str) -> Result<u32, String> {
    match value.parse::<u32>() {
        Ok(parsed) if parsed > 0 => Ok(parsed),
        Ok(_) => Err("value must be at least 1".to_owned()),
        Err(error) => Err(error.to_string()),
    }
}

/// Parses a count that must be at least two.
///
/// A compaction job of one block rewrites that block and leaves the same rows
/// behind, which is the loop a planner must not enter.
/// [`CompactionPolicy`](krabka_blockstore::CompactionPolicy) clamps its fan-in
/// cap anyway; refusing the value here tells the operator instead of silently
/// correcting them.
///
/// # Errors
///
/// Returns the parse error for text that is not a `usize`, and an error for
/// `0` and `1`.
pub fn parse_min_two_usize(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(parsed) if parsed >= 2 => Ok(parsed),
        Ok(_) => Err("value must be at least 2".to_owned()),
        Err(error) => Err(error.to_string()),
    }
}

/// Parses the per-connection dispatch queue capacity of a Krabka client.
///
/// # Errors
///
/// Returns the parse error for text that is not a `usize`, and the
/// [`ConnectionDispatchQueueCapacity`] error for a capacity it refuses.
pub fn parse_client_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let capacity = value.parse::<usize>().map_err(|error| error.to_string())?;
    ConnectionDispatchQueueCapacity::new(capacity).map(ConnectionDispatchQueueCapacity::get)
}

/// Parses the largest frame a Krabka client sends or accepts.
///
/// # Errors
///
/// Returns the [`parse::positive_byte_size`] error for text that is not a
/// positive byte size, and the [`ClientFrameMax`] error for a size it refuses.
pub fn parse_client_frame_max(value: &str) -> Result<ByteSize, String> {
    let size = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    ClientFrameMax::try_from(size).map(ClientFrameMax::size)
}

/// Parses a WAL consumer fetch size, which must fit a Kafka fetch request.
///
/// # Errors
///
/// Returns the [`parse::positive_byte_size`] error for text that is not a
/// positive byte size, and the [`ConsumerFetchMaxBytes`] error for a size it
/// refuses.
pub fn parse_consumer_fetch_size(value: &str) -> Result<ByteSize, String> {
    let size = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    ConsumerFetchMaxBytes::try_from(size)?;
    Ok(size)
}

/// Parses a positive byte size that is a whole number of bytes no larger than
/// 2^53, so the `f64` inside [`ByteSize`] holds it exactly.
///
/// # Errors
///
/// Returns the [`parse::positive_byte_size`] error for text that is not a
/// positive byte size, and an error for a fractional byte count or one above
/// 2^53.
pub fn parse_positive_whole_byte_size(value: &str) -> Result<ByteSize, String> {
    let size = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    let bytes = size.bytes_f64();
    if bytes.fract() != 0.0 || bytes > MAX_EXACT_WHOLE_BYTES {
        return Err(
            "size must be a positive whole-byte value exactly representable by UOM".to_owned(),
        );
    }
    Ok(size)
}

#[cfg(test)]
mod tests;
