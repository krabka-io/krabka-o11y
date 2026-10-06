use std::{collections::hash_map::RandomState, hash::BuildHasher};

use super::{
    BTreeMap, COL_SPAN_ID, COL_START, COL_TRACE_ID, HashSet, RecordBatch, Result, UnixNano,
    fixed_8, fixed_16, i64_value,
};

// A keyed random priority chooses each unique span with equal probability.
// This has the same one-span-per-trace distribution as Tempo's reservoir.
pub(crate) fn sample_exemplar_rows(
    batches: &[RecordBatch],
    start: UnixNano,
    end: UnixNano,
    limit: usize,
) -> Result<HashSet<(usize, usize)>> {
    let random = RandomState::new();
    sample_rows(batches, start, end, limit, |trace, span| {
        random.hash_one((trace, span))
    })
}

fn sample_rows(
    batches: &[RecordBatch],
    start: UnixNano,
    end: UnixNano,
    limit: usize,
    priority: impl Fn(&[u8; 16], &[u8]) -> u64,
) -> Result<HashSet<(usize, usize)>> {
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
            let choice = traces.entry(trace).or_insert((score, batch_index, row));
            if score < choice.0 {
                *choice = (score, batch_index, row);
            }
        }
    }
    Ok(traces
        .into_values()
        .map(|(_, batch, row)| (batch, row))
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

    fn batch(rows: &[(u8, u8, i64)]) -> RecordBatch {
        let mut traces = FixedSizeBinaryBuilder::new(16);
        let mut spans = FixedSizeBinaryBuilder::new(8);
        for (trace, span, _) in rows {
            traces.append_value([*trace; 16]).unwrap();
            spans.append_value([*span; 8]).unwrap();
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
                    rows.iter()
                        .map(|(_, _, timestamp)| *timestamp)
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn each_trace_gets_one_choice_across_batches_without_admitting_outside_spans() {
        let batches = vec![
            batch(&[(1, 9, 10), (1, 5, 20), (1, 2, 30), (2, 1, 101)]),
            batch(&[(2, 4, 40), (3, 1, 50), (1, 2, 30)]),
        ];
        let selected = sample_rows(&batches, UnixNano(0), UnixNano(100), 2, |_, span| {
            u64::from(span[0])
        })
        .unwrap();
        assert!(selected == HashSet::from([(0, 2), (1, 0)]));
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
            assert!(selected == HashSet::from([(0, row)]));
        }
    }
}
