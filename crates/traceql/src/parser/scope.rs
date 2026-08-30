use super::{Result, Scope, TraceqlError};

pub(crate) fn scope(s: &str) -> Result<Scope> {
    match s {
        "span" => Ok(Scope::Span),
        "resource" => Ok(Scope::Resource),
        "parent" => Ok(Scope::Parent),
        "event" => Ok(Scope::Event),
        "link" => Ok(Scope::Link),
        "instrumentation" => Ok(Scope::Instrumentation),
        _ => Err(TraceqlError::Parse(format!("unknown scope {s:?}"))),
    }
}
