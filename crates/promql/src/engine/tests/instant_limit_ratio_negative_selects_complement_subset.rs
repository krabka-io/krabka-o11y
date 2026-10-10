#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_limit_ratio_negative_selects_complement_subset() {
    let selected = selected_memory_instances("limit_ratio(-0.25, memory_bytes)").await;
    assert2::assert!(selected == vec!["e"]);
}
