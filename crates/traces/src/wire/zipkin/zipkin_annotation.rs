use super::*;

#[derive(Deserialize)]
pub(crate) struct ZipkinAnnotation {
    pub(crate) timestamp: i64,
    pub(crate) value: String,
}
