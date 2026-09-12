use super::NativeSymbol;

pub(crate) fn loader_frames(loader: &addr2line::Loader, address: u64) -> Option<Vec<NativeSymbol>> {
    let mut frames = loader.find_frames(address).ok()?;
    let mut out = Vec::new();
    while let Some(frame) = frames.next().ok()? {
        let location = frame.location;
        let function = frame
            .function
            .and_then(|function| function.demangle().ok().map(std::borrow::Cow::into_owned))
            .or_else(|| loader.find_symbol(address).map(ToString::to_string))
            .unwrap_or_default();
        let file = location
            .as_ref()
            .and_then(|location| location.file)
            .unwrap_or_default()
            .to_string();
        let line = location
            .and_then(|location| location.line)
            .and_then(|line| i32::try_from(line).ok())
            .unwrap_or_default();
        if !function.is_empty() || !file.is_empty() || line != 0 {
            out.push(NativeSymbol {
                function,
                file,
                line,
            });
        }
    }
    Some(out)
}
