#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_limitk_selects_deterministic_hash_subset() {
    let selected = selected_memory_instances("limitk(2, memory_bytes)").await;
    assert2::assert!(selected == vec!["d", "e"]);
}
