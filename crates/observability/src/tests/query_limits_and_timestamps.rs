use krabka_units::convert::TimeExt as _;

use super::prelude::{BTreeMap, BTreeSet, BlockIndex, HttpQueryError, LabelIndex, Labels, check};

// === split-modules: generated submodules ===
mod a_loki_stream_interval_keeps_the_first_entry_of_each_window;
mod a_native_timestamp_may_be_the_epoch_but_not_before_it;
mod a_prometheus_duration_sums_its_chunks_in_nanoseconds;
mod a_volume_query_range_is_capped_at_its_limit_exactly;
mod counting_stream_lines_stops_before_its_bound_but_keeps_odd_entries;
mod every_per_query_limit_admits_exactly_its_boundary;
mod hex_rendering_puts_the_high_nibble_first;
mod only_an_object_store_failure_is_classified_as_retryable;
