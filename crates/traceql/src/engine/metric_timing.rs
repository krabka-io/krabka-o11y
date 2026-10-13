use krabka_units::Time;

#[derive(Clone, Copy)]
pub(crate) struct MetricGrid {
    pub(crate) start_ns: i64,
    pub(crate) step_ns: i64,
}

#[derive(Clone, Copy)]
pub(crate) struct ExemplarRange {
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
}

#[derive(Clone, Copy)]
pub(crate) struct MetricExemplarPolicy<'a> {
    pub(crate) max_exemplars_per_series: usize,
    pub(crate) histogram_buckets: &'a [Time],
}

impl<'a> MetricExemplarPolicy<'a> {
    pub(crate) fn new(max_exemplars_per_series: usize, histogram_buckets: &'a [Time]) -> Self {
        Self {
            max_exemplars_per_series,
            histogram_buckets,
        }
    }
}
