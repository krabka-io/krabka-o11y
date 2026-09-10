//! Input builders shared by the fuzz targets.
//!
//! A target that hands libFuzzer's bytes straight to a decoder spends most of
//! its budget being rejected by the outermost length or framing check. The
//! builders here take the same bytes and shape them into a structurally
//! well-formed message whose *values* the fuzzer chooses, so the run reaches
//! the arithmetic and the allocation sizing behind the framing.
//!
//! Both halves are wanted, so each decoder that has a builder also has a
//! raw-bytes target beside it.

pub mod remote_write;
pub mod thrift;
