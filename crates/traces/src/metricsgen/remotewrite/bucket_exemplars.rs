use super::Exemplar;

pub(crate) fn bucket_exemplars(
    exemplars: &[Exemplar],
    assigned: &mut [bool],
    mut predicate: impl FnMut(&Exemplar) -> bool,
) -> Vec<Exemplar> {
    let mut out = Vec::new();
    for indexed_exemplar in exemplars.iter().enumerate() {
        let idx = indexed_exemplar.0;
        let exemplar = indexed_exemplar.1;
        if !assigned[idx] && predicate(exemplar) {
            assigned[idx] = true;
            out.push(exemplar.clone());
        }
    }
    out
}
