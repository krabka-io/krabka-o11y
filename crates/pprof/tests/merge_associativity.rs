use krabka_pprof::{Frame, Tree};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn merge_is_order_independent(stacks in stack_samples()) {
        let mut whole = Tree::new();
        for sample in &stacks {
            whole.add_stack(&sample.frames, sample.value);
        }

        let mid = stacks.len() / 2;
        let mut left = Tree::new();
        for sample in &stacks[..mid] {
            left.add_stack(&sample.frames, sample.value);
        }
        let mut right = Tree::new();
        for sample in &stacks[mid..] {
            right.add_stack(&sample.frames, sample.value);
        }
        left.merge(&right);

        prop_assert_eq!(whole.to_flamegraph(2048), left.to_flamegraph(2048));
    }
}

#[derive(Debug)]
struct StackSample {
    frames: Vec<Frame>,
    value: i64,
}

fn stack_samples() -> impl Strategy<Value = Vec<StackSample>> {
    prop::collection::vec(
        stack_frames().prop_flat_map(|frames| {
            (1_i64..1_000).prop_map(move |value| StackSample {
                frames: frames.clone(),
                value,
            })
        }),
        1..40,
    )
}

fn stack_frames() -> impl Strategy<Value = Vec<Frame>> {
    prop::collection::vec("[a-z]{1,8}", 1..8).prop_map(|names| {
        names
            .into_iter()
            .map(|name| Frame {
                function: name,
                file: String::new(),
                line: 0,
            })
            .collect()
    })
}
