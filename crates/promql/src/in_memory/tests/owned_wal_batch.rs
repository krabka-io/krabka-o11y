use std::sync::Arc;

use super::*;

#[test]
fn owned_batch_moves_labels_and_preserves_rows_snapshots_and_watermarks() {
    let head = WalHead::new();
    let before = head.snapshot();
    let first = WalRecord {
        tenant: "t".into(),
        labels: [("b", "old"), ("a", "=x\n"), ("b", "last")]
            .map(|(k, v)| (k.into(), v.into()))
            .into(),
        payload: SamplePayload::Float {
            timestamp_ms: 100,
            value: 1.0,
            start_timestamp_ms: Some(50),
        },
        exemplars: Vec::new(),
    };
    let buffers = [first.labels[1].1.as_ptr(), first.labels[2].1.as_ptr()];
    let labels = lbls(&[("a", "=x\n"), ("b", "last")]);
    let other = lbls(&[("é", "🦀\0"), ("", ""), ("x", "y=z\n")]);
    let record = |tenant: &str, labels: &Labels, payload, exemplars| WalRecord {
        tenant: tenant.into(),
        labels: labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        payload,
        exemplars,
    };
    head.apply_owned_wal_records_at([
        (first, PartitionIndex(0), Offset(7)),
        (
            record(
                "t",
                &labels,
                SamplePayload::Hist {
                    timestamp_ms: 200,
                    hist: native_histogram(),
                },
                Vec::new(),
            ),
            PartitionIndex(0),
            Offset(8),
        ),
        (
            record(
                "other",
                &other,
                SamplePayload::Float {
                    timestamp_ms: 150,
                    value: 3.0,
                    start_timestamp_ms: None,
                },
                Vec::new(),
            ),
            PartitionIndex(1),
            Offset(20),
        ),
        (
            record(
                "t",
                &Labels::new(),
                SamplePayload::Metadata {
                    metric_family_name: "m".into(),
                    metric_type: "gauge".into(),
                    help: "help".into(),
                    unit: "seconds".into(),
                },
                Vec::new(),
            ),
            PartitionIndex(1),
            Offset(21),
        ),
        (
            record(
                "t",
                &labels,
                SamplePayload::Exemplars,
                vec![WalExemplar {
                    labels: vec![("trace_id".into(), "abc".into())],
                    timestamp_ms: 300,
                    value: 4.0,
                }],
            ),
            PartitionIndex(0),
            Offset(9),
        ),
    ]);
    let after = head.snapshot();
    let mut floats = after
        .floats
        .iter()
        .flat_map(|(tenant, rows)| {
            rows.iter().map(move |r| {
                (
                    tenant.as_str(),
                    r.fp,
                    r.labels.as_ref().clone(),
                    r.ts_ms,
                    r.value.to_bits(),
                    r.start_timestamp_ms,
                )
            })
        })
        .collect::<Vec<_>>();
    floats.sort_by(|left, right| left.0.cmp(right.0));
    assert2::assert!(
        floats
            == [
                (
                    "other",
                    0x56a3_88ef_237b_8646,
                    other,
                    150,
                    3.0_f64.to_bits(),
                    None
                ),
                (
                    "t",
                    0xd1e5_33a9_4f60_f896,
                    labels.clone(),
                    100,
                    1.0_f64.to_bits(),
                    Some(50)
                ),
            ]
    );
    let first = after.floats["t"].iter().next().unwrap();
    assert2::assert!(first.labels.get("a").unwrap().as_ptr() == buffers[0]);
    assert2::assert!(first.labels.get("b").unwrap().as_ptr() == buffers[1]);
    assert2::assert!(after.hists.len() == 1 && after.hists["t"].len() == 1);
    let hist = after.hists["t"].iter().next().unwrap();
    assert2::assert!(
        (
            hist.fp,
            hist.labels.as_ref(),
            hist.ts_ms,
            hist.hist.as_ref()
        ) == (0xd1e5_33a9_4f60_f896, &labels, 200, &native_histogram())
    );
    assert2::assert!(Arc::ptr_eq(&first.labels, &hist.labels));
    assert2::assert!(after.exemplars.len() == 1 && after.exemplars["t"].len() == 1);
    let exemplar = after.exemplars["t"].iter().next().unwrap();
    assert2::assert!(
        (
            exemplar.series_labels.as_ref(),
            exemplar.labels.as_ref(),
            exemplar.ts_ms,
            exemplar.value
        ) == (&labels, &lbls(&[("trace_id", "abc")]), 300, 4.0)
    );
    assert2::assert!(Arc::ptr_eq(&first.labels, &exemplar.series_labels));
    assert2::assert!(after.metadata.len() == 1 && after.metadata["t"].len() == 1);
    let m = after.metadata["t"].iter().next().unwrap();
    assert2::assert!(
        (&*m.metric_family_name, &*m.metric_type, &*m.help, &*m.unit)
            == ("m", "gauge", "help", "seconds")
    );
    assert2::assert!(
        before.floats.is_empty()
            && before.hists.is_empty()
            && before.metadata.is_empty()
            && before.exemplars.is_empty()
    );
    assert2::assert!(
        after
            .watermarks
            .iter()
            .map(|(p, w)| (*p, w.low_water_offset, w.high_water_offset))
            .collect::<Vec<_>>()
            == vec![
                (PartitionIndex(0), Offset(7), Offset(9)),
                (PartitionIndex(1), Offset(20), Offset(21))
            ]
    );
    let held = Arc::downgrade(&first.labels);
    head.delete_tenant("t");
    assert2::assert!(head.snapshot().floats.get("t").is_none());
    assert2::assert!(held.upgrade().is_some());
    drop(after);
    assert2::assert!(held.upgrade().is_none());
}

#[test]
fn owned_batch_panic_does_not_publish_a_prefix_or_poison_later_writes() {
    let head = WalHead::new();
    let labels = lbls(&[("app", "api")]);
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        head.apply_owned_wal_records_at(
            std::iter::once((
                float_record("t", &labels, 100, 1.0),
                PartitionIndex(0),
                Offset(7),
            ))
            .chain(std::iter::from_fn(|| panic!("failed after first record"))),
        );
    }));
    assert2::assert!(panicked.is_err());
    assert2::assert!(head.snapshot().floats.is_empty());
    assert2::assert!(head.high_water_offset(PartitionIndex(0)).is_none());
    head.apply_owned_wal_records_at([(
        float_record("t", &labels, 200, 2.0),
        PartitionIndex(0),
        Offset(8),
    )]);
    let snapshot = head.snapshot();
    assert2::assert!(
        snapshot.floats["t"]
            .iter()
            .map(|r| (r.labels.as_ref().clone(), r.ts_ms, r.value))
            .collect::<Vec<_>>()
            == vec![(labels, 200, 2.0)]
    );
    assert2::assert!(head.low_water_offset(PartitionIndex(0)) == Some(Offset(8)));
    assert2::assert!(head.high_water_offset(PartitionIndex(0)) == Some(Offset(8)));
}
