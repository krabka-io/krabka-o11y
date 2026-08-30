use super::*;

#[derive(Clone, Debug, Default)]
pub struct AddressFallbackResolver;

impl NativeResolver for AddressFallbackResolver {
    fn symbolize(&self, request: &SymbolizeRequest) -> Option<Vec<NativeSymbol>> {
        Some(vec![NativeSymbol {
            function: format!("{}+0x{:x}", build_label(request), request.address),
            file: request.filename.clone(),
            line: 0,
        }])
    }
}
