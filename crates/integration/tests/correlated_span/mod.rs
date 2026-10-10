//! The trace and span ids that the cross-signal correlation suites carry from
//! one signal's path into another's.

/// The trace the correlated records belong to. Suites write it as hex, the way
/// an OpenTelemetry-instrumented emitter writes it, and it has to survive back
/// out as the same 16 bytes `krabka-traces` keys a trace on.
pub const TRACE_ID: [u8; 16] = [
    0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36,
];

/// The span within [`TRACE_ID`] that the correlated records name.
pub const SPAN_ID: [u8; 8] = [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7];
