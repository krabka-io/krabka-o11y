/// One block that has fallen outside its tenant's retention window.
///
/// The tenant travels with the key because the caller has to drop the block
/// from that tenant's index before it deletes the object, and an object key
/// alone does not say which index holds it.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ExpiredBlock {
    pub tenant: String,
    pub object_key: String,
}
