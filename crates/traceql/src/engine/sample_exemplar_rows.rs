use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash, Hasher},
};

use super::{
    BTreeMap, COL_SPAN_ID, COL_START, COL_TRACE_ID, HashSet, RecordBatch, Result, UnixNano,
    fixed_8, fixed_16, i64_value,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ExemplarRow {
    pub(crate) batch_index: usize,
    pub(crate) row_index: usize,
}

struct ExemplarCandidate {
    score: u64,
    row: ExemplarRow,
}

// A keyed random priority chooses each unique span with equal probability.
// This has the same one-span-per-trace distribution as Tempo's reservoir.
pub(crate) fn sample_exemplar_rows(
    batches: &[RecordBatch],
    start: UnixNano,
    end: UnixNano,
    limit: usize,
) -> Result<HashSet<ExemplarRow>> {
    let random = RandomState::new();
    sample_rows(batches, start, end, limit, |trace, span| {
        let mut hasher = random.build_hasher();
        trace.hash(&mut hasher);
        span.hash(&mut hasher);
        hasher.finish()
    })
}

fn sample_rows(
    batches: &[RecordBatch],
    start: UnixNano,
    end: UnixNano,
    limit: usize,
    priority: impl Fn(&[u8; 16], &[u8]) -> u64,
) -> Result<HashSet<ExemplarRow>> {
    let mut traces = BTreeMap::new();
    if limit == 0 {
        return Ok(HashSet::new());
    }
    for (batch_index, batch) in batches.iter().enumerate() {
        for row in 0..batch.num_rows() {
            let timestamp = UnixNano(i64_value(batch, COL_START, row)?);
            if timestamp < start || timestamp > end {
                continue;
            }
            let trace = fixed_16(batch, COL_TRACE_ID, row)?;
            if !traces.contains_key(&trace) && traces.len() >= limit {
                continue;
            }
            let span = fixed_8(batch, COL_SPAN_ID, row)?;
            let score = priority(&trace, &span);
            let choice = traces.entry(trace).or_insert(ExemplarCandidate {
                score,
                row: ExemplarRow {
                    batch_index,
                    row_index: row,
                },
            });
            if score < choice.score {
                choice.score = score;
                choice.row = ExemplarRow {
                    batch_index,
                    row_index: row,
                };
            }
        }
    }
    Ok(traces
        .into_values()
        .map(|candidate| candidate.row)
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{FixedSizeBinaryBuilder, Int64Array},
        datatypes::{DataType, Field, Schema},
    };
    use assert2::assert;

    use super::*;

    #[derive(bon::Builder)]
    struct SpanSampleRow {
        trace: u8,
        span: u8,
        timestamp: i64,
    }

    fn sample_row(trace: u8, span: u8, timestamp: i64) -> SpanSampleRow {
        SpanSampleRow::builder()
            .trace(trace)
            .span(span)
            .timestamp(timestamp)
            .build()
    }

    fn batch(rows: &[SpanSampleRow]) -> RecordBatch {
        let mut traces = FixedSizeBinaryBuilder::new(16);
        let mut spans = FixedSizeBinaryBuilder::new(8);
        for row in rows {
            traces.append_value([row.trace; 16]).unwrap();
            spans.append_value([row.span; 8]).unwrap();
        }
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), false),
                Field::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), false),
                Field::new(COL_START, DataType::Int64, false),
            ])),
            vec![
                Arc::new(traces.finish()),
                Arc::new(spans.finish()),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.timestamp).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn each_trace_gets_one_choice_across_batches_without_admitting_outside_spans() {
        let batches = vec![
            batch(&[
                sample_row(1, 9, 10),
                sample_row(1, 5, 20),
                sample_row(1, 2, 30),
                sample_row(2, 1, 101),
            ]),
            batch(&[
                sample_row(2, 4, 40),
                sample_row(3, 1, 50),
                sample_row(1, 2, 30),
            ]),
        ];
        let selected = sample_rows(&batches, UnixNano(0), UnixNano(100), 2, |_, span| {
            u64::from(span[0])
        })
        .unwrap();
        assert!(
            selected
                == HashSet::from([
                    ExemplarRow {
                        batch_index: 0,
                        row_index: 2
                    },
                    ExemplarRow {
                        batch_index: 1,
                        row_index: 0
                    },
                ])
        );
        assert!(
            sample_rows(&batches, UnixNano(0), UnixNano(100), 0, |_, _| 0)
                .unwrap()
                .is_empty()
        );
        // Every distinct span can win; expanded duplicates retain its priority.
        for winner in [2, 5, 9] {
            let selected = sample_rows(&batches, UnixNano(0), UnixNano(100), 1, |_, span| {
                u64::from(span[0] != winner)
            })
            .unwrap();
            let row = match winner {
                9 => 0,
                5 => 1,
                _ => 2,
            };
            assert!(
                selected
                    == HashSet::from([ExemplarRow {
                        batch_index: 0,
                        row_index: row,
                    }])
            );
        }
    }
}
