use super::{Serialize, Deserialize};

/// A run of populated buckets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketSpan {
    pub offset: i32,
    pub length: u32,
}
