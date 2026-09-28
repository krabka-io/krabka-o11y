/// The leading and trailing zero counts that a Gorilla XOR stream carries
/// from one value to the next.
#[derive(Clone, Copy, Debug, Default)]
pub struct XorState {
    pub leading: u8,
    pub trailing: u8,
}
