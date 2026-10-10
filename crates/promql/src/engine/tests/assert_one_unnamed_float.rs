use super::*;

/// One label's name and value.
#[derive(Clone, Copy)]
pub(crate) struct ExpectedLabel<'a> {
    pub(crate) name: &'a str,
    pub(crate) label_value: &'a str,
}

/// Checks that `samples` is one float sample without a metric name, carrying
/// the `label`, whose value is approximately `expected`.
pub(crate) fn assert_one_unnamed_float(
    samples: &[crate::InstantSample],
    label: ExpectedLabel<'_>,
    expected: f64,
) {
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.get("__name__").is_none());
    assert2::assert!(samples[0].labels.get(label.name) == Some(label.label_value));
    assert2::assert!(approx_eq(float_value(&samples[0].value), expected));
}
