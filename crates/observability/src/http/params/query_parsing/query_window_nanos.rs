use super::{
    DecodedQueryPair, HttpQueryError, parse_loki_duration_query_param,
    parse_loki_timestamp_query_param,
};

/// The first `start`, `end` and `since` a query string carries, in
/// nanoseconds. A repeated parameter keeps its first value, as Loki's does.
#[derive(Default)]
pub(crate) struct QueryWindowNanos {
    pub(crate) start: Option<i64>,
    pub(crate) end: Option<i64>,
    pub(crate) since: Option<i64>,
}

impl QueryWindowNanos {
    /// Parses `pair` when it is the first `start`, `end` or `since`, and
    /// ignores every other pair.
    pub(crate) fn record(&mut self, pair: &DecodedQueryPair) -> Result<(), HttpQueryError> {
        let value = pair.value.as_str();
        match pair.key.as_str() {
            "start" if self.start.is_none() => {
                self.start = Some(parse_loki_timestamp_query_param("start", value)?);
            }
            "end" if self.end.is_none() => {
                self.end = Some(parse_loki_timestamp_query_param("end", value)?);
            }
            "since" if self.since.is_none() => {
                self.since = Some(parse_loki_duration_query_param("since", value)?);
            }
            _ => {}
        }
        Ok(())
    }
}
