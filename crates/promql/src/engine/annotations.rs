use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
};

use crate::result::Annotations;

tokio::task_local! {
    /// Per-query annotation sink.
    ///
    /// Each public query entry point scopes this once. The deeply recursive
    /// evaluation path can then record warnings and infos without a collector
    /// argument at every call site.
    pub(crate) static ANNOTATIONS: RefCell<Annotations>;
}

// === split-modules: generated submodules ===
mod emit_info;
mod emit_warning;
mod incompatible_types_in_binop_info;
mod invalid_quantile_warning;
mod invalid_ratio_warning;
mod is_valid_quantile;
mod mixed_classic_native_warning;
mod warn_mixed_histograms;

pub (super) use emit_info::emit_info;
pub (super) use emit_warning::emit_warning;
pub (super) use incompatible_types_in_binop_info::incompatible_types_in_binop_info;
pub (super) use invalid_quantile_warning::invalid_quantile_warning;
# [cfg (feature = "experimental-functions")] pub (super) use invalid_ratio_warning::invalid_ratio_warning;
pub (super) use is_valid_quantile::is_valid_quantile;
use mixed_classic_native_warning::mixed_classic_native_warning;
pub (super) use warn_mixed_histograms::warn_mixed_histograms;
