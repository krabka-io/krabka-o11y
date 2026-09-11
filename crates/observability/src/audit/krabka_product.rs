use super::ProductInfo;

/// The vendor name in every Krabka audit record. The broker uses the same name.
const VENDOR_NAME: &str = "Krabka";

/// The product identity that each audit record carries, for the service
/// `name` at `version`.
///
/// A service binary should pass its package name and
/// `env!("CARGO_PKG_VERSION")`.
#[must_use]
pub fn krabka_product(name: &str, version: &str) -> ProductInfo {
    ProductInfo {
        vendor_name: VENDOR_NAME.to_owned(),
        name: name.to_owned(),
        version: version.to_owned(),
    }
}
