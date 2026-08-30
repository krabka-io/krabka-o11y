use super::*;

#[derive(Debug, Default)]
pub(crate) struct SeriesParams {
    pub(crate) matchers: Vec<String>,
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
    pub(crate) since: Option<i64>,
}
