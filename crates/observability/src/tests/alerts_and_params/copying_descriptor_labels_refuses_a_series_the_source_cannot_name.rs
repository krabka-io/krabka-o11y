use super::*;

/// `insert_descriptor_labels` copies a block's series labels from one index
/// to another, and REFUSES when the source cannot supply them. A missing
/// series is a corrupt index rather than an empty block, so carrying on
/// would write a manifest whose blocks reference series nothing can name.
#[test]
pub(crate) fn copying_descriptor_labels_refuses_a_series_the_source_cannot_name() {
    use krabka_blockstore::{BlockDescriptor, BlockKey, LabelIndex, TimeRange};

    let mut source = LabelIndex::default();
    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    let known = source.insert_series("tenant", labels.clone());
    let mut other = Labels::default();
    other.insert("app".to_string(), "web".to_string());
    let also_known = source.insert_series("tenant", other.clone());

    let descriptor = |fingerprints: &[_]| {
        BlockDescriptor::new(
            BlockKey::new("tenant", 0, 0, 1, TimeRange::new(0, 10).expect("a range")),
            fingerprints.iter().copied().collect(),
        )
    };

    // Both series are known, so both are copied.
    let mut target = LabelIndex::default();
    super::super::prelude::insert_descriptor_labels(
        &mut target,
        &source,
        "tenant",
        &descriptor(&[known, also_known]),
    )
    .expect("both series are known");
    check!(target.labels_for("tenant", known) == Some(&labels));
    check!(target.labels_for("tenant", also_known) == Some(&other));

    // A fingerprint the source has never seen is refused, and the error
    // names which one so the corruption can be found.
    let mut target = LabelIndex::default();
    let stranger = LabelIndex::default().insert_series("tenant", {
        let mut labels = Labels::default();
        labels.insert("app".to_string(), "stranger".to_string());
        labels
    });
    check!(matches!(
        super::super::prelude::insert_descriptor_labels(
            &mut target,
            &source,
            "tenant",
            &descriptor(&[stranger])
        ),
        Err(CompactorRunError::MissingSeriesLabels { .. })
    ));

    // The labels belong to a TENANT, so the right fingerprint under the
    // wrong tenant is just as unknown.
    let mut target = LabelIndex::default();
    check!(
        super::super::prelude::insert_descriptor_labels(
            &mut target,
            &source,
            "other",
            &descriptor(&[known])
        )
        .is_err(),
        "a fingerprint is not global"
    );

    // A descriptor with no series copies nothing and succeeds.
    let mut target = LabelIndex::default();
    super::super::prelude::insert_descriptor_labels(&mut target, &source, "tenant", &descriptor(&[]))
        .expect("an empty descriptor is not an error");
    check!(target.labels_for("tenant", known).is_none());
}
