use super::*;

/// A `case` label value and the float the case carries.
#[derive(Clone, Copy)]
pub(crate) struct CaseValue {
    pub(crate) case: &'static str,
    pub(crate) sample_value: f64,
}

impl CaseValue {
    /// The `case` and the float it carries.
    pub(crate) const fn new(case: &'static str, sample_value: f64) -> Self {
        Self { case, sample_value }
    }
}

/// Pushes one `tenant-a` float sample at 10s per case, labelled with the
/// metric `metric_name` and that case.
pub(crate) fn push_cases(store: &mut InMemoryMetricStore, metric_name: &str, cases: &[CaseValue]) {
    for &CaseValue { case, sample_value } in cases {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", metric_name), ("case", case)]),
            10_000,
            sample_value,
        );
    }
}

/// Checks that `samples` holds exactly one unnamed sample per expected case,
/// each approximately that case's value.
pub(crate) fn assert_case_values(samples: &[crate::InstantSample], expected: &[CaseValue]) {
    assert2::assert!(samples.len() == expected.len());
    for &CaseValue { case, sample_value } in expected {
        let sample = samples
            .iter()
            .find(|sample| sample.labels.get("case") == Some(case))
            .expect("sample for case");
        assert2::assert!(sample.labels.get("__name__") == None);
        assert2::assert!(approx_eq(float_value(&sample.value), sample_value));
    }
}
