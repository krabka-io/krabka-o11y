use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use datafusion::{catalog::MemTable, prelude::SessionContext};
use krabka_blockstore::SeriesFingerprint;
use krabka_metrics::{
    encode_float_samples, encode_native_histograms, float_sample_schema, native_histogram_schema,
};

use super::{
    InMemoryMetricStore, RowChunks,
    matcher::{all_match, prepare_matchers, row_matches},
};
use crate::{
    PromqlError, PromqlLabels as Labels, PromqlMatcher as LabelMatcher,
    error::Result,
    store::{
        ExemplarRecord, ExemplarScan, LabelNameCardinality, LabelValueCardinality, MetadataScan,
        MetricStore, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
    },
};

mod in_memory_metric_store;
use crate::series_stats::{SeriesRef, label_name_cardinality, label_value_cardinality, tsdb_stats};

impl InMemoryMetricStore {
    /// Each float and histogram row's series fingerprint and labels for
    /// `tenant`, floats first, in ingest order.
    fn tenant_series_rows<'a>(
        &'a self,
        tenant: &str,
    ) -> impl Iterator<Item = SeriesRef<'a>> + use<'a> {
        let floats = self.floats.get(tenant).into_iter().flat_map(|rows| {
            rows.iter().map(|row| SeriesRef {
                fp: row.fp,
                labels: row.labels.as_ref(),
            })
        });
        let hists = self.hists.get(tenant).into_iter().flat_map(|rows| {
            rows.iter().map(|row| SeriesRef {
                fp: row.fp,
                labels: row.labels.as_ref(),
            })
        });
        floats.chain(hists)
    }
}
