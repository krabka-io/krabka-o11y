//! Which provider an object-store URL reaches, for the reports a contract or
//! lifecycle run writes.

use object_store::ObjectStoreScheme;
use url::Url;

/// The cloud whose client serves `url`: `aws`, `gcs`, `azure`, or `other`.
///
/// `object_store` picks the client from the scheme, and for an `https://` URL
/// from the host. The URL scheme alone does not name the cloud.
#[must_use]
pub fn object_store_cloud(url: &Url) -> &'static str {
    let Ok(parsed) = ObjectStoreScheme::parse(url) else {
        return "other";
    };
    match parsed.0 {
        ObjectStoreScheme::AmazonS3 => "aws",
        ObjectStoreScheme::GoogleCloudStorage => "gcs",
        ObjectStoreScheme::MicrosoftAzure => "azure",
        _ => "other",
    }
}

/// The host of the endpoint override in the environment, or `None` for the
/// provider's default.
///
/// The URL host of `s3://`, `gs://` and `az://` names a bucket or container,
/// so an S3-compatible store is only visible here.
#[must_use]
pub fn object_store_endpoint_host() -> Option<String> {
    ["AWS_ENDPOINT", "AWS_ENDPOINT_URL", "AZURE_STORAGE_ENDPOINT"]
        .into_iter()
        .find_map(|name| std::env::var(name).ok())
        .and_then(|raw| Url::parse(&raw).ok())
        .and_then(|endpoint| endpoint.host_str().map(str::to_owned))
}
