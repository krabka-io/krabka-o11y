/// Whether a leaf batch carries the sample-time duplicate of its timestamp
/// column after the value column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SampleTimePresence {
    /// The instant-selector leaf: the sample time rides the chain unchanged.
    Included,
    /// The range leaves, whose windowed UDFs read only timestamp and value.
    Omitted,
}
