use super::*;

pub(crate) fn gcd_signed(left: i128, right: u128) -> u128 {
    let mut left = left.unsigned_abs();
    let mut right = right;
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}
