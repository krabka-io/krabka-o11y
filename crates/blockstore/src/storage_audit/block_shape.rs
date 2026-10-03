/// What a block key says about the block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockShape {
    /// The WAL partition, when the key names one.
    pub partition: Option<i32>,
    /// The metrics block kind, or an empty string for the other signals.
    /// Blocks of different lanes never share WAL offsets.
    pub lane: String,
    /// The inclusive WAL offset range, when the key names one. A compacted
    /// or rewritten block has none.
    pub offsets: Option<(i64, i64)>,
    /// The inclusive record time range in nanoseconds, when the key names
    /// one. The audit reads it from logs block keys only.
    pub time_range: Option<(i64, i64)>,
}
