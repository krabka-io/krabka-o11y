#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VolumeAggregateBy {
    Series,
    Labels,
}
