use super::{BTreeSet, BinModifier, InstantSample, SetOp, binary_match_key};

pub(crate) fn eval_vector_set_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: SetOp,
    modifier: Option<&BinModifier>,
) -> Vec<InstantSample> {
    let mut left_keys = BTreeSet::new();
    let mut right_keys = BTreeSet::new();
    for sample in &left {
        left_keys.insert(binary_match_key(&sample.labels, modifier));
    }
    for sample in &right {
        right_keys.insert(binary_match_key(&sample.labels, modifier));
    }

    let mut out = Vec::new();
    match op {
        SetOp::And => {
            for sample in left {
                if right_keys.contains(&binary_match_key(&sample.labels, modifier)) {
                    out.push(sample);
                }
            }
        }
        SetOp::Unless => {
            for sample in left {
                if !right_keys.contains(&binary_match_key(&sample.labels, modifier)) {
                    out.push(sample);
                }
            }
        }
        SetOp::Or => {
            out.extend(left);
            for sample in right {
                if !left_keys.contains(&binary_match_key(&sample.labels, modifier)) {
                    out.push(sample);
                }
            }
        }
    }
    out
}
