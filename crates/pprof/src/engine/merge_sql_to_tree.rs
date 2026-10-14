use super::{
    Arc, Frame, ProfileError, StacktraceIdentity, Tree, selected_sample_rows,
    selected_sample_rows::SelectedSampleRow, stack_matches_call_sites,
};

pub(crate) async fn merge_sql_to_tree(
    scan: &crate::ProfileScan,
    sql: &str,
    tree: &mut Tree,
    prefix_frames: &[Frame],
    call_sites: &[String],
    trace_ids: Option<&[Vec<u8>]>,
) -> Result<(), ProfileError> {
    let batches = scan.collect_sql(sql).await?;
    for batch in batches {
        let rows = selected_sample_rows(&batch, trace_ids)?;
        let symbols = Arc::clone(&scan.symbols);
        let resolved = tokio::task::spawn_blocking(move || {
            let mut resolved = Vec::<ResolvedStack>::new();
            for row in rows {
                let SelectedSampleRow {
                    partition,
                    stacktrace_id,
                    value,
                } = row;
                let stack = StacktraceIdentity::new(partition, stacktrace_id);
                if let Some(previous) = resolved.last_mut()
                    && previous.stack == stack
                {
                    previous.values.push(value);
                } else {
                    resolved.push(ResolvedStack {
                        stack,
                        frames: symbols.resolve(partition, stacktrace_id),
                        values: vec![value],
                    });
                }
            }
            resolved
        })
        .await
        .map_err(|err| ProfileError::Symbolize(format!("symbolization worker failed: {err}")))?;
        for ResolvedStack {
            mut frames, values, ..
        } in resolved
        {
            if call_sites.is_empty() || stack_matches_call_sites(&frames, call_sites) {
                frames.extend_from_slice(prefix_frames);
                // Keep individual values and their order, including negative
                // and zero samples, while reusing the resolved stack.
                for value in values {
                    tree.add_stack(&frames, value);
                }
            }
        }
    }
    Ok(())
}

// Query SQL orders samples by partition and stack ID. Reuse adjacent stacks
// within each batch; no state outlives the captured symbol source or batch.
struct ResolvedStack {
    stack: StacktraceIdentity,
    frames: Vec<Frame>,
    values: Vec<i64>,
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{BinaryArray, Int64Array, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use assert2::assert;
    use datafusion::{
        catalog::MemTable,
        prelude::{SessionConfig, SessionContext},
    };

    use super::*;
    use crate::{ProfileScan, SymbolSource};

    struct Symbols;

    struct StackExpectation {
        frames: Vec<&'static str>,
        value: i64,
    }

    struct CallSiteExpectation {
        sites: Vec<&'static str>,
        indices: Vec<usize>,
    }

    fn frames(names: &[&str]) -> Vec<Frame> {
        names
            .iter()
            .map(|name| Frame {
                function: (*name).to_string(),
                file: "source.rs".to_string(),
                line: 1,
            })
            .collect()
    }

    impl SymbolSource for Symbols {
        fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> {
            if partition == 0 && id == 1 {
                frames(&["a", "main"])
            } else if partition == 1 && id == 1 {
                frames(&["b", "main"])
            } else if partition == 0 && id == 3 {
                frames(&["inline_leaf", "inline_outer", "main"])
            } else {
                Vec::new()
            }
        }
    }

    fn sample_scan() -> ProfileScan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("partition", DataType::UInt64, false),
            Field::new("stack", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("trace", DataType::Binary, true),
            Field::new("ordinal", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(vec![0, 0, 0, 1, 0, 0, 0, 0, 0])),
                Arc::new(UInt64Array::from(vec![
                    1,
                    1,
                    1,
                    1,
                    2,
                    3,
                    1,
                    u64::from(u32::MAX) + 1,
                    1,
                ])),
                Arc::new(Int64Array::from(vec![3, -4, 0, 5, 7, 2, 6, 999, 999])),
                // Seven rows of the wanted profile type, then one of another
                // type and one with none.
                Arc::new(
                    std::iter::repeat_n(Some(&b"wanted"[..]), 7)
                        .chain([Some(&b"other"[..]), None])
                        .collect::<BinaryArray>(),
                ),
                Arc::new(UInt64Array::from_iter_values(0..9)),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
        ctx.register_table(
            "samples",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        ProfileScan {
            ctx,
            samples_table: "samples".to_string(),
            symbols: Arc::new(Symbols),
        }
    }

    #[tokio::test]
    async fn repeated_stacks_keep_partition_values_inline_frames_and_prefix_filtering() {
        let scan = sample_scan();
        let prefix = frames(&["group"]);
        let wanted = [b"wanted".to_vec()];
        let stacks = [
            StackExpectation {
                frames: vec!["a", "main", "group"],
                value: 5,
            },
            StackExpectation {
                frames: vec!["b", "main", "group"],
                value: 5,
            },
            StackExpectation {
                frames: vec!["group"],
                value: 7,
            },
            StackExpectation {
                frames: vec!["inline_leaf", "inline_outer", "main", "group"],
                value: 2,
            },
        ];
        for expectation in [
            CallSiteExpectation {
                sites: vec![],
                indices: vec![0, 1, 2, 3],
            },
            CallSiteExpectation {
                sites: vec!["main"],
                indices: vec![0, 1, 3],
            },
            CallSiteExpectation {
                sites: vec!["main", "inline_outer"],
                indices: vec![3],
            },
            CallSiteExpectation {
                sites: vec!["group"],
                indices: vec![],
            },
        ] {
            let sites = expectation
                .sites
                .into_iter()
                .map(str::to_string)
                .collect::<Vec<_>>();
            let mut actual = Tree::new();
            merge_sql_to_tree(
                &scan,
                "SELECT partition, stack, value, trace FROM samples ORDER BY ordinal",
                &mut actual,
                &prefix,
                &sites,
                Some(&wanted),
            )
            .await
            .unwrap();
            let mut expected_tree = Tree::new();
            for index in expectation.indices {
                let expected = &stacks[index];
                expected_tree.add_stack(&frames(&expected.frames), expected.value);
            }
            assert!(actual.to_flamegraph(i64::MAX) == expected_tree.to_flamegraph(i64::MAX));
        }
        let mut invalid = Tree::new();
        assert!(
            merge_sql_to_tree(
                &scan,
                "SELECT partition, stack, value, trace FROM samples ORDER BY ordinal",
                &mut invalid,
                &prefix,
                &[],
                None
            )
            .await
            .is_err()
        );
    }
}
