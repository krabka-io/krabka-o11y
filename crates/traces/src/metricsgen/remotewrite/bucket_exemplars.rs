use super::*;

pub(crate) fn bucket_exemplars(
    exemplars: &[Exemplar],
    assigned: &mut [bool],
    mut predicate: impl FnMut(&Exemplar) -> bool,
) -> Vec<Exemplar> {
    let mut out = Vec::new();
    for (idx, exemplar) in exemplars.iter().enumerate() {
        if !assigned[idx] && predicate(exemplar) {
            assigned[idx] = true;
            out.push(exemplar.clone());
        }
    }
    out
}
