/// Leaf-batch and projection column that carries the per-series sample timestamp.
///
/// The scalar-math functions report the timestamp of the inner sample unchanged,
/// because the interpreter function `eval_unary_float_call` keeps `sample.ts_ms`.
/// The projection therefore carries the timestamp with the value.
pub const SAMPLE_TIME_COLUMN: &str = "sample_timestamp";
