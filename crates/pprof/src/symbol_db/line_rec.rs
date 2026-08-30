use super::{Deserialize, Serialize};

/// One inlined line within a location.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LineRec {
    pub function_id: u32,
    pub line: i32,
}
