use super::*;

#[test]
pub(crate) fn compactor_configured_object_store_builds_when_not_injected() {
    let object_store_dir = tempfile::tempdir().unwrap();
    let object_store_url = Url::from_directory_path(object_store_dir.path())
        .expect("temporary directory should be representable as a file URL")
        .to_string();
    let config = ServiceConfig::parse_from([
        "krabka-observability",
        "--target",
        "compactor",
        "--object-store-url",
        &object_store_url,
    ]);

    let configured_store = build_compactor_configured_object_store(&config, None)
        .expect("valid object-store URL should configure a compactor store");

    assert!(
        configured_store.is_some(),
        "compactor should build the configured object store when no store is injected"
    );
}
