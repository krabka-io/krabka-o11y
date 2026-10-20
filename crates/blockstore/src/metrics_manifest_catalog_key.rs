//! Placement of compact metrics manifest summaries.
use std::fmt::Write as _;

use crate::{escape_object_path_segment, input_key_fingerprint};

const LEAF_SHIFT: u32 = 24;

fn ordered_time(timestamp: i64) -> u64 {
    u64::from_be_bytes(timestamp.to_be_bytes()) ^ (1_u64 << 63)
}

fn root(prefix: &str, tenant: &str) -> String {
    let tenant = escape_object_path_segment(tenant);
    let prefix = prefix.trim_end_matches('/');
    if prefix.is_empty() {
        format!("{tenant}/_catalog/v1/")
    } else {
        format!("{prefix}/{tenant}/_catalog/v1/")
    }
}

/// Returns the summary key for a metrics manifest's identity and bounds.
#[must_use]
pub fn metrics_manifest_catalog_key(
    tenant: &str,
    index_key: &str,
    min_ts: i64,
    max_ts: i64,
    timeless: bool,
) -> String {
    let tenant_segment = format!("{}/", escape_object_path_segment(tenant));
    let tenant_path = format!("/{tenant_segment}");
    let prefix = index_key
        .split_once(&tenant_path)
        .map_or("", |parts| parts.0);
    let mut directory = root(prefix, tenant);
    if timeless {
        directory.push_str("timeless/");
    } else {
        directory.push_str("time/");
        let first = ordered_time(min_ts) >> LEAF_SHIFT;
        let last = ordered_time(max_ts) >> LEAF_SHIFT;
        for shift in (0..10).rev().map(|level| level * 4) {
            let left = (first >> shift) & 15;
            if left != (last >> shift) & 15 {
                break;
            }
            write!(&mut directory, "{left:x}/").expect("writing to a String cannot fail");
        }
        directory.push_str("entries/");
    }
    format!(
        "{directory}{:016x}-{:016x}-{:016x}.catalog",
        ordered_time(min_ts),
        ordered_time(max_ts),
        input_key_fingerprint(&[index_key.to_owned()])
    )
}

/// Whether a directory names a TSDB import gated by a publication marker.
#[must_use]
pub fn is_metrics_import_directory(directory: &str) -> bool {
    let Some(directory_parts) = directory.rsplit_once('/') else {
        return false;
    };
    let parent = directory_parts.0;
    let Some(name_parts) = directory_parts.1.split_once('-') else {
        return false;
    };
    let ulid = name_parts.0;
    let digits = name_parts.1;
    parent.ends_with("/uploaded")
        && ulid.len() == 26
        && ulid
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte.is_ascii_uppercase())
        && digits.len() == 16
        && digits
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
