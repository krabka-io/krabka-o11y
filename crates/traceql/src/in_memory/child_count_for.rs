use super::*;

pub(crate) fn child_count_for(nested_sets: &[NestedSet], idx: usize) -> i32 {
    let Some(nested) = nested_sets.get(idx) else {
        return 0;
    };
    i32::try_from(
        nested_sets
            .iter()
            .filter(|other| other.parent_id == nested.left)
            .count(),
    )
    .unwrap_or(i32::MAX)
}
