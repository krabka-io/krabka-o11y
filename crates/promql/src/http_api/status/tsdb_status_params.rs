use super::Deserialize;

#[derive(Debug, Default, Deserialize)]
pub(crate) struct TsdbStatusParams {
    pub(crate) limit: Option<usize>,
}
