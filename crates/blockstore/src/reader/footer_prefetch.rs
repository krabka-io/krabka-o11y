use super::{ByteSize, kibibytes};

/// How much of a block's tail one footer read asks for.
///
/// The probe knows the block size from its `head`, so it reads the tail as one
/// bounded range. One read then holds the footer, the metadata and the page
/// index of a typical metric block, and a later query that wants the page
/// index finds it in the footer cache. A block whose metadata is larger than
/// this costs one more read.
pub(crate) const FOOTER_PREFETCH: ByteSize = kibibytes(64);
