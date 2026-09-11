/// The byte that introduces an escape in an object-store path segment.
///
/// [`super::escape_object_path_segment`] documents why it is `!` and not `%`.
pub(super) const PATH_ESCAPE_MARKER: u8 = b'!';
