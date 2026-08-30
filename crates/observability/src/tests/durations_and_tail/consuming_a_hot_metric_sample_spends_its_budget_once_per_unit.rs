use super::*;

/// `consume_hot_metric_sample` spends one unit of a per-series, per-instant
/// budget, and reports whether it could. Its three refusals are distinct
/// causes -- the sample has no timestamp, the series and instant were never
/// counted, or their budget is already spent -- and all three return the
/// same false, so each is reached separately here.
///
/// The decrement is the point: consuming twice from a budget of one must
/// succeed then fail. A test that consumed once could not tell a decrement
/// from a mere presence check.
#[test]
pub(crate) fn consuming_a_hot_metric_sample_spends_its_budget_once_per_unit() {
    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    let other = Labels::default();
    let sample = serde_json::json!([1_700_000_000, "1"]);
    let key = |labels: &Labels| (labels.clone(), "1700000000".to_string());

    let mut counts = BTreeMap::new();
    counts.insert(key(&labels), 2_u64);

    // Two units budgeted, so two succeed and the third does not.
    check!(super::super::prelude::consume_hot_metric_sample(
        &mut counts,
        &labels,
        &sample
    ));
    check!(super::super::prelude::consume_hot_metric_sample(
        &mut counts,
        &labels,
        &sample
    ));
    check!(
        !super::super::prelude::consume_hot_metric_sample(&mut counts, &labels, &sample),
        "the budget is spent, not merely present"
    );
    check!(counts[&key(&labels)] == 0, "and it stops at zero");

    // A different series has its own budget, not this one's.
    check!(
        !super::super::prelude::consume_hot_metric_sample(&mut counts, &other, &sample),
        "an uncounted series has nothing to spend"
    );

    // A different instant of the SAME series likewise: the key is the pair.
    let later = serde_json::json!([1_700_000_001, "1"]);
    check!(!super::super::prelude::consume_hot_metric_sample(
        &mut counts,
        &labels,
        &later
    ));

    // A sample with no timestamp at all.
    check!(!super::super::prelude::consume_hot_metric_sample(
        &mut counts,
        &labels,
        &serde_json::json!([])
    ));
    check!(!super::super::prelude::consume_hot_metric_sample(
        &mut counts,
        &labels,
        &serde_json::json!("bare")
    ));
}
