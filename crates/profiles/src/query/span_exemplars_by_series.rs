use super::{BTreeMap, SeriesKey, pb};

pub(crate) type SpanExemplarsBySeries =
    BTreeMap<SeriesKey, BTreeMap<i64, Vec<pb::types::v1::Exemplar>>>;
