use super::*;

/// `append_matching_log_row` decides whether one row belongs in the
/// response. Its first guard is three conditions or-ed as REJECTIONS --
/// too early, too late, or not a series the plan wants -- so each rejects
/// on its own against a row the other two accept.
///
/// Both range bounds are inclusive, pinned by rows sitting exactly on
/// each. A row whose series the label index cannot name is an ERROR rather
/// than a skip: the plan asked for that series, so being unable to label
/// it means the index disagrees with the plan.
#[test]
pub(crate) fn a_log_row_is_appended_only_when_the_plan_asked_for_it() {
    use krabka_logql::StreamPlan;

    let mut label_index = LabelIndex::default();
    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    let known = label_index.insert_series("tenant", labels);
    let mut other = Labels::default();
    other.insert("app".to_string(), "web".to_string());
    let unwanted = label_index.insert_series("tenant", other);

    let plan = StreamPlan {
        tenant: "tenant".to_string(),
        time_range: krabka_blockstore::TimeRange::new(10, 90).expect("a valid range"),
        query: krabka_logql::parse_query("{app=\"api\"}").expect("the query parses"),
        fingerprints: [known].into_iter().collect(),
        blocks: Vec::new(),
    };
    let metadata = Labels::default();
    let appended = |fingerprint, timestamp_ns| {
        let mut streams = BTreeMap::new();
        let result = super::super::prelude::append_matching_log_row(
            &mut streams,
            &plan,
            &label_index,
            super::super::prelude::QueryRow {
                fingerprint,
                timestamp_ns,
                line: "line",
                structured_metadata: &metadata,
            },
            &[],
        );
        result.map(|()| streams.values().map(Vec::len).sum::<usize>())
    };

    // Inside the range, and a series the plan wants.
    check!(appended(known, 50).ok() == Some(1));
    // Exactly on each bound: both inclusive.
    check!(
        appended(known, 10).ok() == Some(1),
        "the start bound is inclusive"
    );
    check!(appended(known, 90).ok() == Some(1), "and so is the end");
    // One step outside each.
    check!(appended(known, 9).ok() == Some(0), "before the range");
    check!(appended(known, 91).ok() == Some(0), "after it");
    // A series the plan did not ask for, inside the range.
    check!(
        appended(unwanted, 50).ok() == Some(0),
        "not a wanted series"
    );

    // A fingerprint the label index cannot name is an error, not a skip --
    // but only once the row has passed the range and series filters, so a
    // nameless series the plan never wanted is still simply skipped.
    let nameless = 999_999_u64;
    check!(
        appended(nameless, 50).ok() == Some(0),
        "not wanted, so not named"
    );
    let mut wants_nameless = plan.clone();
    wants_nameless.fingerprints.insert(nameless);
    let mut streams = BTreeMap::new();
    check!(matches!(
        super::super::prelude::append_matching_log_row(
            &mut streams,
            &wants_nameless,
            &label_index,
            super::super::prelude::QueryRow {
                fingerprint: nameless,
                timestamp_ns: 50,
                line: "line",
                structured_metadata: &metadata,
            },
            &[],
        ),
        Err(super::super::prelude::QueryError::MissingSeriesLabels { .. })
    ));
}
