use super::{Object, ObjectSymbol};

pub(crate) fn nearest_symbol_name(object: &object::File<'_>, address: u64) -> Option<String> {
    object
        .symbols()
        .filter(|symbol| symbol.address() <= address)
        .filter(|symbol| {
            let size = symbol.size();
            size == 0 || address < symbol.address().saturating_add(size)
        })
        .max_by_key(object::ObjectSymbol::address)
        .and_then(|symbol| symbol.name().ok())
        .map(ToString::to_string)
}
