use std::{
    fmt,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use futures::StreamExt as _;
use krabka_units::{ByteSize, mebibytes};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use refined_type::rule::GreaterUsize;
use tracing::instrument;

use crate::error::{BlockStoreError, Result};

#[cfg(test)]
mod tests {

    /// `Display` is how the retention reaches config output and log lines.
    /// Writing nothing still succeeds, and reports a retention of "".
    #[test]
    fn retain_displays_its_value() {
        let retain = IndexSnapshotRetain::new(7).expect("7 is a positive retention");
        assert2::check!(retain.to_string() == "7");
        assert2::check!(IndexSnapshotRetain::default().to_string() != "");
    }
    use krabka_units::{convert::ByteSizeExt as _, mebibytes};

    use super::{DEFAULT_INDEX_SNAPSHOT_MAX, IndexSnapshotRetain};

    #[test]
    fn index_snapshot_settings_preserve_defaults_and_validate_input() {
        assert_eq!(DEFAULT_INDEX_SNAPSHOT_MAX.bytes_u64(), 256 * 1024 * 1024);
        assert_eq!(IndexSnapshotRetain::default().into_value(), 8);
        assert_eq!(DEFAULT_INDEX_SNAPSHOT_MAX, mebibytes(256));
        assert_eq!(
            "1".parse::<IndexSnapshotRetain>()
                .expect("one retained snapshot is valid")
                .into_value(),
            1
        );

        for invalid in ["0", "not-a-number", "-1", "18446744073709551616"] {
            assert!(
                invalid.parse::<IndexSnapshotRetain>().is_err(),
                "{invalid:?} should be rejected"
            );
        }
    }
}

mod default_index_snapshot_max;
mod default_index_snapshot_retain;
mod index_snapshot_prefix_for_key;
mod index_snapshot_retain;
mod latest_index_snapshot_path;
mod list_index_snapshot_objects;
mod next_snapshot_key;
mod prune_old_index_snapshots;
mod put_index_snapshot;
mod snapshot_counter;

pub use default_index_snapshot_max::DEFAULT_INDEX_SNAPSHOT_MAX;
pub use default_index_snapshot_retain::DEFAULT_INDEX_SNAPSHOT_RETAIN;
pub use index_snapshot_prefix_for_key::index_snapshot_prefix_for_key;
pub use index_snapshot_retain::IndexSnapshotRetain;
pub use latest_index_snapshot_path::latest_index_snapshot_path;
pub use list_index_snapshot_objects::list_index_snapshot_objects;
use next_snapshot_key::next_snapshot_key;
use prune_old_index_snapshots::prune_old_index_snapshots;
pub use put_index_snapshot::put_index_snapshot;
use snapshot_counter::SNAPSHOT_COUNTER;
