use super::{Intrinsic, Scope};

pub(crate) fn intrinsic_name(scope: &Scope) -> &'static str {
    match scope {
        Scope::Intrinsic(Intrinsic::TraceId) => "trace:id",
        Scope::Intrinsic(Intrinsic::Id) => "span:id",
        Scope::Intrinsic(Intrinsic::ParentId) => "span:parentID",
        Scope::Intrinsic(Intrinsic::Kind) => "span:kind",
        Scope::Intrinsic(Intrinsic::Status) => "span:status",
        _ => "intrinsic",
    }
}
