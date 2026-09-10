/// Which JSON encoding a Loki `streams` response uses for its entries.
///
/// The wire choice is Loki's, and a request makes it with the
/// `X-Loki-Response-Encoding-Flags` header.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum LokiStreamEncoding {
    /// Loki's default: an entry's structured metadata and its parsed labels are
    /// folded into the stream's label map, and every entry stays two elements
    /// long.
    #[default]
    Folded,
    /// What `X-Loki-Response-Encoding-Flags: categorize-labels` asks for: the
    /// stream keeps only its own labels, and each entry carries the rest in a
    /// third element, bucketed by where it came from.
    CategorizeLabels,
}
