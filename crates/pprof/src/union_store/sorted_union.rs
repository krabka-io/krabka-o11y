use super::BTreeSet;

pub(crate) fn sorted_union<const N: usize>(values: [Vec<String>; N]) -> Vec<String> {
    values
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
