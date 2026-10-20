use std::{cell::RefCell, cmp::Ordering, hint::black_box, time::Instant};

use assert2::assert;
use promql_parser::{label::Labels as LabelNames, parser::token::T_BOTTOMK};
use proptest::prelude::*;

use super::*;
use crate::{PromqlLabels as Labels, engine::annotations::ANNOTATIONS, result::Annotations};

fn sample(value: f64, position: i64, group: &str, label: Vec<u8>) -> InstantSample {
    let mut labels = Labels::new();
    labels.insert("__name__", "metric");
    labels.insert("group", group);
    labels.insert("instance", label);
    InstantSample {
        labels,
        ts_ms: position,
        value: SampleValue::Float(value),
        drop_name: position % 2 == 0,
    }
}

// Keep the previous full stable sort and allocating comparator as an independent
// oracle, including the exact float bits and the input order of equal samples.
fn full_sort(
    samples: Vec<InstantSample>,
    op: TokenType,
    k: usize,
    modifier: Option<&LabelModifier>,
) -> Vec<InstantSample> {
    let mut groups = BTreeMap::<String, Vec<InstantSample>>::new();
    for sample in samples {
        if matches!(sample.value, SampleValue::Histogram(_)) {
            continue;
        }
        let key = labels_key(&aggregate_labels(&sample.labels, modifier));
        groups.entry(key).or_default().push(sample);
    }
    let mut out = Vec::new();
    for mut group in groups.into_values() {
        group.sort_by(|left, right| {
            let SampleValue::Float(left_value) = left.value else {
                unreachable!()
            };
            let SampleValue::Float(right_value) = right.value else {
                unreachable!()
            };
            let by_value = match (left_value.is_nan(), right_value.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) if op.id() == T_TOPK => right_value.total_cmp(&left_value),
                (false, false) => left_value.total_cmp(&right_value),
            };
            by_value.then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
        });
        group.truncate(k);
        out.extend(group);
    }
    out
}

fn assert_samples(actual: &[InstantSample], expected: &[InstantSample]) {
    assert!(actual.len() == expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert!(actual.labels == expected.labels);
        assert!(actual.ts_ms == expected.ts_ms);
        assert!(actual.drop_name == expected.drop_name);
        let SampleValue::Float(actual) = actual.value else {
            unreachable!()
        };
        let SampleValue::Float(expected) = expected.value else {
            unreachable!()
        };
        assert!(actual.to_bits() == expected.to_bits());
    }
}

fn check_all_groupings(samples: &[InstantSample], k: usize) {
    for modifier in [
        None,
        Some(LabelModifier::Include(LabelNames::new(vec!["group"]))),
        Some(LabelModifier::Exclude(LabelNames::new(vec!["instance"]))),
    ] {
        for op in [T_TOPK, T_BOTTOMK].map(TokenType::new) {
            let expected = full_sort(samples.to_vec(), op, k, modifier.as_ref());
            let actual = apply_k_aggregate(samples.to_vec(), op, k, modifier.as_ref());
            assert_samples(&actual, &expected);
        }
    }
}

#[test]
fn selection_preserves_special_values_exact_ties_and_group_order() {
    let values = [
        f64::NAN,
        f64::NEG_INFINITY,
        -0.0,
        0.0,
        f64::INFINITY,
        7.0,
        7.0,
        f64::from_bits(0xfff8_0000_0000_0001),
        f64::from_bits(0x7ff8_0000_0000_0002),
    ];
    let samples = (0..108)
        .map(|i| {
            sample(
                values[i % values.len()],
                i64::try_from(i).unwrap(),
                if i % 3 == 0 { "z" } else { "a" },
                b"same".to_vec(),
            )
        })
        .collect::<Vec<_>>();
    for k in [0, 1, 2, 10, 35, 36, 37, 100, 108, 109, usize::MAX] {
        check_all_groupings(&samples, k);
    }
    check_all_groupings(&[], 1);
}

proptest! {
    #[test]
    fn selection_matches_full_sort_with_byte_labels(
        rows in prop::collection::vec((any::<u64>(), prop::collection::vec(any::<u8>(), 0..8), any::<bool>()), 0..128),
        k in 0usize..140,
    ) {
        let samples = rows.into_iter().enumerate().map(|(i, (bits, label, tied))| {
            sample(if tied { 1.0 } else { f64::from_bits(bits) }, i64::try_from(i).unwrap(),
                if i % 3 == 0 { "z" } else { "a" }, label)
        }).collect::<Vec<_>>();
        check_all_groupings(&samples, k);
    }
}

#[test]
fn histograms_keep_annotations_and_zero_k_short_circuit() {
    for op in [T_TOPK, T_BOTTOMK].map(TokenType::new) {
        for k in [0, 1, 10] {
            let mut histogram = sample(0.0, 0, "a", vec![]);
            histogram.value =
                SampleValue::Histogram(krabka_metrics::NativeHistogram::builder().build());
            ANNOTATIONS.sync_scope(RefCell::new(Annotations::default()), || {
                let actual = apply_k_aggregate(
                    vec![histogram.clone(), sample(3.0, 1, "a", vec![]), histogram],
                    op,
                    k,
                    None,
                );
                assert!(actual.len() == usize::from(k != 0));
                ANNOTATIONS.with(|sink| {
                    let expected = if k == 0 {
                        vec![]
                    } else {
                        vec![histogram_ignored_in_aggregation_info(k_aggregate_name(op))]
                    };
                    assert!(sink.borrow().infos == expected);
                });
            });
        }
    }
}

// An opt-in microbenchmark keeps the private production kernel private and
// times it directly, without store selection, planning, or input cloning.
// Run with: cargo test -p krabka-promql --release benchmark_k_selection -- --ignored --nocapture
#[test]
#[ignore = "opt-in topk/bottomk kernel latency benchmark"]
fn benchmark_k_selection() {
    println!("op,distribution,n,k,full_sort_us,selection_us");
    for n in [1_000usize, 10_000, 100_000] {
        for tied in [false, true] {
            let samples = (0..n)
                .map(|i| {
                    // A deterministic permutation avoids sorted-input best cases.
                    let rank = (i * 7919) % n;
                    let value = u32::try_from(if tied { rank % 4 } else { rank }).unwrap();
                    sample(
                        f64::from(value),
                        i64::try_from(i).unwrap(),
                        "a",
                        format!("{i:06}").into_bytes(),
                    )
                })
                .collect::<Vec<_>>();
            for op in [T_TOPK, T_BOTTOMK].map(TokenType::new) {
                for k in [1, 10, 100] {
                    assert_samples(
                        &apply_k_aggregate(samples.clone(), op, k, None),
                        &full_sort(samples.clone(), op, k, None),
                    );
                    let mut micros = [0.0; 2];
                    for (index, elapsed) in micros.iter_mut().enumerate() {
                        for _ in 0..5 {
                            let input = samples.clone();
                            let start = Instant::now();
                            let result = if index == 0 {
                                full_sort(input, op, k, None)
                            } else {
                                apply_k_aggregate(input, op, k, None)
                            };
                            *elapsed += start.elapsed().as_secs_f64() * 1e6;
                            black_box(result);
                        }
                        *elapsed /= 5.0;
                    }
                    println!(
                        "{},{},{n},{k},{:.3},{:.3}",
                        k_aggregate_name(op),
                        if tied { "tied" } else { "varied" },
                        micros[0],
                        micros[1]
                    );
                }
            }
        }
    }
}
