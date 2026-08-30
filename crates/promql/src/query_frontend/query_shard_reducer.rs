#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryShardReducer {
    First,
    Sum,
    Min,
    Max,
}
