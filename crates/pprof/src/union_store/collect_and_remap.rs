use std::{collections::HashSet, sync::Arc};

use arrow::{
    array::{Array, AsArray, BooleanArray},
    compute::filter_record_batch,
};

use super::{ProfileError, ProfileScan, RecordBatch, UnionSymbols, remap_partitions};

/// Cold contributors replace only the first identity of each hot sample.
pub(super) fn filter_hot_samples(
    hot: &[RecordBatch],
    cold: &[RecordBatch],
) -> Result<Option<Vec<RecordBatch>>, ProfileError> {
    let schema = crate::profile_samples_schema();
    if hot
        .iter()
        .chain(cold)
        .any(|batch| batch.schema().as_ref() != schema.as_ref())
    {
        return Ok(None);
    }
    let mut cold_ids = HashSet::<&[u8]>::new();
    for batch in cold {
        let identities = batch
            .column_by_name(crate::PCOL_WAL_SAMPLE_IDS)
            .expect("canonical profile schema has WAL identities")
            .as_list::<i32>();
        let values = identities.values().as_binary::<i32>();
        for row in 0..batch.num_rows() {
            if identities.is_null(row) {
                continue;
            }
            // Sliced lists still own child values outside the visible rows.
            let start = usize::try_from(identities.value_offsets()[row])
                .expect("Arrow validates nonnegative list offsets");
            let end = usize::try_from(identities.value_offsets()[row + 1])
                .expect("Arrow validates nonnegative list offsets");
            for index in start..end {
                if !values.is_null(index) {
                    cold_ids.insert(values.value(index));
                }
            }
        }
    }
    if cold_ids.is_empty() {
        return Ok(Some(hot.to_vec()));
    }
    hot.iter()
        .map(|batch| {
            let identities = batch
                .column_by_name(crate::PCOL_WAL_SAMPLE_IDS)
                .expect("canonical profile schema has WAL identities")
                .as_list::<i32>();
            let values = identities.values().as_binary::<i32>();
            let keep = BooleanArray::from_iter((0..batch.num_rows()).map(|row| {
                if identities.is_null(row) || identities.value_length(row) == 0 {
                    return true;
                }
                let first = usize::try_from(identities.value_offsets()[row])
                    .expect("Arrow validates nonnegative list offsets");
                values.is_null(first) || !cold_ids.contains(values.value(first))
            }));
            if keep.true_count() == batch.num_rows() {
                Ok(batch.clone())
            } else {
                filter_record_batch(batch, &keep)
                    .map_err(|err| ProfileError::Store(err.to_string()))
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

pub(crate) async fn collect_and_remap(
    scan: ProfileScan,
    source_id: u64,
    symbols: &mut UnionSymbols,
) -> Result<Vec<RecordBatch>, ProfileError> {
    let partition_base = source_id << 56;
    symbols.insert(partition_base, Arc::clone(&scan.symbols));
    let sql = format!("SELECT * FROM {}", scan.samples_table);
    let batches = scan.collect_sql(&sql).await?;
    batches
        .into_iter()
        .map(|batch| remap_partitions(&batch, partition_base))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{
            ArrayRef, BinaryArray, BinaryBuilder, Int64Array, ListBuilder, StringDictionaryBuilder,
            UInt64Array,
        },
        compute::concat_batches,
        datatypes::Int32Type,
    };
    use assert2::assert;
    use datafusion::catalog::MemTable;

    use super::{ProfileScan, RecordBatch, UnionSymbols, collect_and_remap, filter_hot_samples};
    use crate::{SymbolDb, SymbolSource, profile_samples_schema, profile_session_context};

    fn batch(partitions: [u64; 3]) -> RecordBatch {
        let mut types = StringDictionaryBuilder::<Int32Type>::new();
        for profile_type in ["cpu:cpu:nanoseconds:cpu:nanoseconds", "memory", "cpu"] {
            types.append(profile_type).unwrap();
        }
        let mut identities = ListBuilder::new(BinaryBuilder::new());
        for row in [&[1_u8, 2][..], &[][..], &[1][..]] {
            for identity in row {
                identities.values().append_value([*identity; 52]);
            }
            identities.append(true);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(vec![9, 1, 9])),
            Arc::new(Int64Array::from(vec![20, 10, 20])),
            Arc::new(types.finish()),
            Arc::new(UInt64Array::from(vec![1, 2, 1])),
            Arc::new(Int64Array::from(vec![-7, 8, 0])),
            Arc::new(UInt64Array::from(partitions.to_vec())),
            Arc::new(Int64Array::from(vec![-70, 80, 0])),
            Arc::new(UInt64Array::from(vec![None, Some(0), Some(u64::MAX)])),
            Arc::new(BinaryArray::from(vec![
                None,
                Some(&[0, 255][..]),
                Some(&[][..]),
            ])),
            Arc::new(identities.finish()),
        ];
        RecordBatch::try_new(profile_samples_schema(), columns).unwrap()
    }

    fn identity_batch(
        row_numbers: &[u64],
        rows: &[Vec<Option<Vec<u8>>>],
        partition_base: u64,
    ) -> RecordBatch {
        let mut types = StringDictionaryBuilder::<Int32Type>::new();
        let mut identities = ListBuilder::new(BinaryBuilder::new());
        for (&number, row) in row_numbers.iter().zip(rows) {
            types
                .append(if number % 2 == 0 { "cpu" } else { "memory" })
                .unwrap();
            for identity in row {
                identities.values().append_option(identity.as_deref());
            }
            identities.append(true);
        }
        let numbers = row_numbers
            .iter()
            .map(|&row| i64::try_from(row).unwrap())
            .collect::<Vec<_>>();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(row_numbers.to_vec())),
            Arc::new(Int64Array::from_iter_values(
                numbers.iter().map(|&row| 100 + row),
            )),
            Arc::new(types.finish()),
            Arc::new(UInt64Array::from_iter_values(
                row_numbers.iter().map(|row| row % 3),
            )),
            Arc::new(Int64Array::from_iter_values(
                numbers.iter().map(|&row| -row),
            )),
            Arc::new(UInt64Array::from_iter_values(
                row_numbers.iter().map(|row| partition_base | (row << 48)),
            )),
            Arc::new(Int64Array::from_iter_values(
                numbers.iter().map(|&row| -10 * row),
            )),
            Arc::new(UInt64Array::from_iter(
                row_numbers.iter().map(|row| (row % 2 == 0).then_some(0)),
            )),
            Arc::new(BinaryArray::from_iter(
                row_numbers
                    .iter()
                    .map(|row| (row % 2 == 1).then_some(&[0, 255][..])),
            )),
            Arc::new(identities.finish()),
        ];
        RecordBatch::try_new(profile_samples_schema(), columns).unwrap()
    }

    #[tokio::test]
    async fn cold_identity_filter_matches_complete_rows_and_the_sql_handoff() {
        let hot_ids = vec![
            vec![Some(vec![1])],
            vec![Some(vec![2])],
            vec![],
            vec![None],
            vec![Some(vec![9]), Some(vec![1])],
            vec![Some(vec![1]), Some(vec![9])],
            vec![Some(vec![])],
            vec![Some(vec![70])],
            vec![Some(vec![80])],
            vec![Some(vec![8])],
            vec![None, Some(vec![2])],
        ];
        let cold_ids = vec![
            vec![Some(vec![70])], // Invisible prefix must not suppress hot.
            vec![Some(vec![1]), None, Some(vec![2])],
            vec![Some(vec![])],
            vec![Some(vec![1])],
            vec![Some(vec![80])], // Invisible suffix must not suppress hot.
        ];
        let hot = identity_batch(&(0..11).collect::<Vec<_>>(), &hot_ids, 1 << 56);
        let cold = identity_batch(&(100..105).collect::<Vec<_>>(), &cold_ids, 2 << 56).slice(1, 3);
        let cold_batches = vec![cold.clone(), cold.slice(0, 1)];
        let filtered = filter_hot_samples(std::slice::from_ref(&hot), &cold_batches)
            .unwrap()
            .unwrap();
        let keep = [2, 3, 4, 7, 8, 9, 10];
        let expected_hot = identity_batch(
            &keep,
            &keep
                .iter()
                .map(|&row| hot_ids[usize::try_from(row).unwrap()].clone())
                .collect::<Vec<_>>(),
            1 << 56,
        );
        assert!(filtered == vec![expected_hot.clone()]);
        let expected = concat_batches(
            &profile_samples_schema(),
            [expected_hot.clone(), cold.clone(), cold.slice(0, 1)].iter(),
        )
        .unwrap();
        let mut actual = filtered;
        actual.extend(cold_batches.clone());
        assert!(concat_batches(&profile_samples_schema(), &actual).unwrap() == expected);

        let ctx = profile_session_context();
        for (name, batches) in [("hot_samples", vec![hot]), ("cold_samples", cold_batches)] {
            ctx.register_table(
                name,
                Arc::new(MemTable::try_new(profile_samples_schema(), vec![batches]).unwrap()),
            )
            .unwrap();
        }
        let reference = ctx
            .sql(
                "SELECT * FROM (SELECT hot.* FROM hot_samples hot WHERE NOT EXISTS (
             SELECT 1 FROM (SELECT UNNEST(wal_sample_ids) AS id FROM cold_samples) cold_ids
             WHERE array_element(hot.wal_sample_ids, 1) = cold_ids.id
             ) UNION ALL SELECT * FROM cold_samples) ORDER BY series_fingerprint",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected_sql = concat_batches(
            &profile_samples_schema(),
            [expected_hot, cold.slice(0, 1), cold].iter(),
        )
        .unwrap();
        assert!(concat_batches(&profile_samples_schema(), &reference).unwrap() == expected_sql);
    }

    #[test]
    fn absent_identities_keep_rows_and_all_matching_hot_rows_are_removed() {
        let hot = identity_batch(
            &[1, 1],
            &[vec![Some(vec![1])], vec![Some(vec![1])]],
            1 << 56,
        );
        let independent = identity_batch(&[2], &[vec![]], 2 << 56);
        assert!(
            filter_hot_samples(std::slice::from_ref(&hot), &[independent]).unwrap()
                == Some(vec![hot.clone()])
        );
        let cold = identity_batch(&[2], &[vec![Some(vec![1])]], 2 << 56);
        let filtered = filter_hot_samples(&[hot.clone(), hot], &[cold])
            .unwrap()
            .unwrap();
        assert!(filtered.len() == 2 && filtered.iter().all(|batch| batch.num_rows() == 0));
    }

    #[test]
    fn noncanonical_schema_retains_the_original_validation_path() {
        let hot = identity_batch(&[1], &[vec![Some(vec![1])]], 1 << 56);
        let missing = hot.project(&(0..9).collect::<Vec<_>>()).unwrap();
        let mut fields = hot.schema().fields().to_vec();
        fields[9] = Arc::new(fields[9].as_ref().clone().with_nullable(true));
        let nullable = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(fields)),
            hot.columns().to_vec(),
        )
        .unwrap();
        for noncanonical in [missing, nullable] {
            assert!(
                filter_hot_samples(
                    std::slice::from_ref(&hot),
                    std::slice::from_ref(&noncanonical)
                )
                .unwrap()
                .is_none()
            );
            assert!(MemTable::try_new(profile_samples_schema(), vec![vec![noncanonical]]).is_err());
        }
    }

    #[tokio::test]
    async fn complete_batches_keep_order_nulls_duplicates_and_the_source_namespace() {
        for (table, source_id, expected_partitions) in [
            (
                "samples",
                1,
                [
                    0x0100_0000_0000_0000,
                    0x0100_0000_0000_0007,
                    0x01ff_0000_0000_0001,
                ],
            ),
            (
                "datafusion.public.samples",
                2,
                [
                    0x0200_0000_0000_0000,
                    0x0200_0000_0000_0007,
                    0x02ff_0000_0000_0001,
                ],
            ),
        ] {
            let input = batch([0, 7, 0x00ff_0000_0000_0001]);
            let ctx = profile_session_context();
            ctx.register_table(
                table,
                Arc::new(MemTable::try_new(input.schema(), vec![vec![input]]).unwrap()),
            )
            .unwrap();
            let source: Arc<dyn SymbolSource> = Arc::new(SymbolDb::new());
            let scan = ProfileScan {
                ctx,
                samples_table: table.into(),
                symbols: Arc::clone(&source),
            };
            let mut symbols = UnionSymbols::default();
            let actual = collect_and_remap(scan, source_id, &mut symbols)
                .await
                .unwrap();
            assert!(actual == vec![batch(expected_partitions)]);
            assert!(symbols.sources.len() == 1);
            assert!(Arc::ptr_eq(&symbols.sources[&(source_id << 56)], &source));
        }
    }
}
