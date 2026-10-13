use super::Ordering;

pub(crate) fn template_compare_values(left: &str, right: &str) -> Option<Ordering> {
    if let Ok(left_number) = left.parse::<f64>() {
        if let Ok(right_number) = right.parse::<f64>()
            && left_number.is_finite()
            && right_number.is_finite()
        {
            return left_number.partial_cmp(&right_number);
        }
    }
    Some(left.cmp(right))
}
