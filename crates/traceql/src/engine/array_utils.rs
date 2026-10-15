use arrow::{array::PrimitiveArray, datatypes::Int64Type};

use super::{
    Array, AsArray, COL_START, ListArray, RecordBatch, Result, TraceqlError,
};

// Bytes To Hex
/// Renders `bytes` as lowercase hex, two digits per byte.
#[must_use]
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

// Fixed Bytes
pub(crate) fn fixed_bytes<const N: usize>(
    batch: &RecordBatch,
    col: &str,
    row: usize,
) -> Result<[u8; N]> {
    batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_fixed_size_binary()
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not {N} bytes")))
}

pub(crate) fn fixed_8(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 8]> {
    fixed_bytes(batch, col, row)
}

pub(crate) fn fixed_16(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 16]> {
    fixed_bytes(batch, col, row)
}

// Optional Fixed 8
pub(crate) fn optional_fixed_8(
    batch: &RecordBatch,
    col: &str,
    row: usize,
) -> Result<Option<[u8; 8]>> {
    let arr = batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?;
    if arr.is_null(row) {
        return Ok(None);
    }
    arr.as_fixed_size_binary()
        .value(row)
        .try_into()
        .map(Some)
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 8 bytes")))
}

// Optional List Column
pub(crate) fn optional_list_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a ListArray>> {
    batch
        .column_by_name(name)
        .map(|col| {
            col.as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| TraceqlError::Exec(format!("nested column `{name}` is not a list")))
        })
        .transpose()
}

// Span Start Column

/// The span start times of a scanned batch, in Unix nanoseconds.
pub(crate) fn span_start_column(batch: &RecordBatch) -> Result<&PrimitiveArray<Int64Type>> {
    Ok(batch
        .column_by_name(COL_START)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {COL_START}")))?
        .as_primitive::<Int64Type>())
}
