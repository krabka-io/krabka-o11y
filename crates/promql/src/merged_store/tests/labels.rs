use super::*;
pub(crate) fn labels(pairs: &[crate::test_support::TestLabelPair<'_>]) -> Labels {
    crate::test_support::labels::labels(pairs).into()
}
