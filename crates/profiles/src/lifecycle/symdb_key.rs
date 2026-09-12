/// The key of the symbol database that sits beside `block_key`.
///
/// Every profile block has one. The block holds stack-trace ids, and the
/// symbol database is what turns them back into function names, so a block
/// whose symbol database is gone still reads and still answers a query, with
/// every frame unnamed. Nothing but the block key names this object: it is in
/// no index, and no listing tells it apart from a block. Anything that moves
/// or deletes a block therefore has to name it here as well.
#[must_use]
pub fn symdb_key(block_key: &str) -> String {
    format!("{block_key}.symdb")
}
