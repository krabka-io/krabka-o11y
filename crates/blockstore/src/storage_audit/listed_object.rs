use super::{ClassifiedObject, ObjectMeta};

/// One object from the listing, with what its key says about it.
#[derive(Clone, Debug)]
pub struct ListedObject {
    pub meta: ObjectMeta,
    pub object: ClassifiedObject,
}
