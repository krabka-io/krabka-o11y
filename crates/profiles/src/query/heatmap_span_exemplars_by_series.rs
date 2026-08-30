use super::{BTreeMap, SeriesKey, pb};

pub(crate) type HeatmapSpanExemplarsBySeries =
    BTreeMap<SeriesKey, BTreeMap<i64, Vec<pb::querier::v1::Exemplar>>>;
