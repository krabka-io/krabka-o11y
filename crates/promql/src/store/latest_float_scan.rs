use std::{collections::BTreeMap, sync::Arc};

use krabka_blockstore::{Labels, SeriesFingerprint};
use krabka_metrics::FloatSampleRow;

/// Latest instant samples and the labels resolved over their label window.
///
/// Labels can cover a wider window than samples, including series at the
/// excluded lookback boundary. Those series still count toward query limits.
pub struct LatestFloatScan {
    /// At most one latest float sample per fingerprint.
    pub samples: Vec<FloatSampleRow>,
    /// Complete matched labels, preserving the store's source precedence.
    pub labels: BTreeMap<SeriesFingerprint, Arc<Labels>>,
}
