use super::{Arc, SessionContext, SymbolSource};

/// A selected samples table plus the symbol source that resolves its raw ids.
pub struct ProfileScan {
    pub ctx: SessionContext,
    pub samples_table: String,
    pub symbols: Arc<dyn SymbolSource>,
}
