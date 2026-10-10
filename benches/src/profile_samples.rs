//! Repeated stack IDs and trace associations for full profile queries.

use krabka_pprof::{
    FlameGraph, Frame, FunctionRec, InMemoryProfileStore, LineRec, LocationRec, Tree,
};

use crate::{Seeded, index::TENANT};

pub const PROFILE_TYPE: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
pub const STACK_DEPTH: usize = 16;
const PARTITIONS: usize = 4;
const TRACES: usize = 16;

/// A sample store and complete expected query results.
pub struct ProfileSamples {
    pub store: InMemoryProfileStore,
    pub traces: Vec<Vec<u8>>,
    pub end_ms: i64,
    pub all: FlameGraph,
    pub one_trace: FlameGraph,
    pub main: FlameGraph,
}

/// Build samples over four symbol partitions and sixteen trace associations.
///
/// Expected trees come from the generated frames and sample ledger, before
/// the query engine or symbol resolver runs.
///
/// # Panics
/// Panics if `stacks_per_partition` is zero or counts do not fit their types.
#[must_use]
pub fn profile_samples(samples: usize, stacks_per_partition: usize) -> ProfileSamples {
    let stacks_per_partition = std::num::NonZeroUsize::new(stacks_per_partition)
        .expect("at least one stack is required")
        .get();
    let mut store = InMemoryProfileStore::new();
    let mut stacks = Vec::new();
    let mut frames = Vec::new();
    for partition in 0..PARTITIONS {
        for stack in 0..stacks_per_partition {
            let generated = (0..STACK_DEPTH)
                .map(|level| Frame {
                    function: if level == STACK_DEPTH - 1 {
                        if stack % 2 == 0 { "main" } else { "worker" }.to_string()
                    } else {
                        format!("partition_{partition}_fn_{level}_{}", stack % (level + 2))
                    },
                    file: format!("src/partition_{partition}/module_{level}.rs"),
                    line: i32::try_from(level + 1).expect("a line fits i32"),
                })
                .collect::<Vec<_>>();
            let db = store.symbols_mut();
            let locations = generated
                .iter()
                .map(|frame| {
                    let name = db.intern_string(&frame.function);
                    let filename = db.intern_string(&frame.file);
                    let function_id = db.intern_function(FunctionRec {
                        name,
                        system_name: name,
                        filename,
                        start_line: i64::from(frame.line),
                    });
                    db.intern_location(LocationRec {
                        address: 0,
                        mapping_id: 0,
                        lines: vec![LineRec {
                            function_id,
                            line: frame.line,
                        }],
                    })
                })
                .collect::<Vec<_>>();
            let partition = u64::try_from(partition).expect("a partition fits u64");
            stacks.push((partition, db.intern_stacktrace(partition, &locations)));
            frames.push(generated);
        }
    }
    let traces = (0..TRACES)
        .map(|trace| vec![u8::try_from(trace + 1).expect("a trace fits u8"); 16])
        .collect::<Vec<_>>();
    let mut totals = vec![[0_i64; 3]; stacks.len()];
    let mut random = Seeded::new(0x05A6_D1E5);
    for row in 0..samples {
        let stack = random.next_below(stacks.len());
        let trace = random.next_below(TRACES);
        let value = i64::try_from(random.next_below(1_000) + 1).expect("a value fits i64");
        totals[stack][0] += value;
        if trace == 0 {
            totals[stack][1] += value;
        }
        if frames[stack]
            .last()
            .is_some_and(|frame| frame.function == "main")
        {
            totals[stack][2] += value;
        }
        store.push_sample_with_total_and_associations(
            (TENANT, PROFILE_TYPE),
            vec![("service".to_string(), "api".to_string())],
            stacks[stack],
            (value, value),
            i64::try_from(row).expect("a timestamp fits i64"),
            (
                Some(u64::try_from(trace + 1).expect("a span fits u64")),
                Some(traces[trace].clone()),
            ),
        );
    }
    let mut expected = [Tree::new(), Tree::new(), Tree::new()];
    for (frames, totals) in frames.iter().zip(totals) {
        for (tree, total) in expected.iter_mut().zip(totals) {
            if total != 0 {
                tree.add_stack(frames, total);
            }
        }
    }
    let [all, one_trace, main] = expected.map(|tree| tree.to_flamegraph(i64::MAX));
    ProfileSamples {
        store,
        traces,
        end_ms: i64::try_from(samples).expect("a timestamp fits i64"),
        all,
        one_trace,
        main,
    }
}
