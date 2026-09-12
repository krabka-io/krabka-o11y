use super::{CompactionJob, MetricBlockKind};

/// One merge the metrics planner asks for.
///
/// The kind travels with the job because a metric block holds one payload kind
/// and one schema. Float rows and native-histogram rows share a fingerprint
/// column and nothing else, so a job that mixed two kinds would have no output
/// schema to write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricCompactionJob {
    /// The payload kind of every input, and of the output.
    pub kind: MetricBlockKind,
    /// The blocks to merge, their level, and the range they cover.
    pub job: CompactionJob,
}
