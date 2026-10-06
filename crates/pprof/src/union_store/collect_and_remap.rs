use super::{ProfileError, ProfileScan, RecordBatch, UnionSymbols, remap_partitions};

pub(crate) async fn collect_and_remap(
    scan: ProfileScan,
    source_id: u64,
    symbols: &mut UnionSymbols,
) -> Result<Vec<RecordBatch>, ProfileError> {
    let partition_base = source_id << 56;
    symbols.insert(partition_base, scan.symbols);
    let sql = format!("SELECT * FROM {}", scan.samples_table);
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
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
        datatypes::Int32Type,
    };
    use assert2::assert;
    use datafusion::catalog::MemTable;

    use super::{ProfileScan, RecordBatch, UnionSymbols, collect_and_remap};
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
