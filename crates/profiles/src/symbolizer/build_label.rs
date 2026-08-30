use super::SymbolizeRequest;

pub(crate) fn build_label(request: &SymbolizeRequest) -> String {
    if request.build_id.is_empty() {
        request.filename.clone()
    } else {
        request.build_id.clone()
    }
}
