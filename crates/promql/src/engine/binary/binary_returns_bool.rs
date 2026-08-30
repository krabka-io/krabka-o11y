use super::BinModifier;

pub(crate) fn binary_returns_bool(modifier: Option<&BinModifier>) -> bool {
    modifier.is_some_and(|modifier| modifier.return_bool)
}
