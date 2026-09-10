//! The differential corpus, read from the vendored Prometheus `.test` files.
//!
//! `crates/promql/tests/testdata` holds the upstream `promql/promqltest`
//! corpus, vendored whole at `v3.8.1`. The in-process conformance suite already
//! runs every `eval` in it against Krabka's engine. This module turns the same
//! files into something two *containers* can be asked about: a `load` block
//! becomes a `remote_write` body, and an `eval` line becomes a query that both
//! Krabka and the real thing answer.
//!
//! The upstream expectations are not read here. A differential compares two
//! engines against each other over identical input, so what the file says the
//! answer is does not enter into it.
//!
//! # Segments
//!
//! A `.test` file is not one dataset. `clear` throws the storage away, and a
//! `load` that follows an `eval` adds to it, so a file is a sequence of
//! *segments*, each of which upstream evaluates against a different store.
//! Neither Prometheus nor Mimir has an operation that empties a tenant -- a
//! tombstone written by Prometheus's delete API masks the samples re-appended
//! over it -- so the separation is done in time instead. Each segment starts
//! an hour past the end of the one before it, which is further than the
//! five-minute lookback of an instant selector can reach. Everything is written
//! once, up front, and every query names a timestamp inside its own segment's
//! window.
//!
//! A range selector longer than the gap does still reach into the segment
//! before it. That is left alone deliberately: both engines are handed exactly
//! the same samples, so the comparison stays honest even where the data is not
//! the data upstream had in mind.
//!
//! The one thing the shift breaks is `@`, whose argument is an absolute
//! timestamp written into the query text. `at_modifier.test` is therefore
//! replayed first, at offset zero, where its literal timestamps still mean what
//! they say; the few `@`-literal cases in other files are skipped by name.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    path::{Path, PathBuf},
};

use assert2::assert;
use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint, wire::pb};
use krabka_promql::{SampleSpec, Statement, parse_test_file};
use krabka_units::convert::TimeExt;
use prost::Message;
use serde_json::Value;

/// Gap between one segment's last sample and the next segment's first.
const SEGMENT_GAP_MS: i64 = 60 * 60 * 1000;

/// Prometheus's stale marker: a quiet NaN with a payload of 2.
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// The vendored files this differential replays, in replay order.
///
/// `at_modifier.test` comes first because its queries carry absolute `@`
/// timestamps throughout, and the first segment is the one that runs unshifted.
/// Everything after it is alphabetical.
const REPLAYED_FILES: &[&str] = &[
    "at_modifier.test",
    "aggregators.test",
    "collision.test",
    "duration_expression.test",
    "extended_vectors.test",
    "functions.test",
    "histograms.test",
    "info.test",
    "literals.test",
    "name_label_dropping.test",
    "native_histograms.test",
    "operators.test",
    "range_queries.test",
    "ranges.test",
    "selectors.test",
    "staleness.test",
    "subquery.test",
    "trig_functions.test",
    "type_and_unit.test",
];

/// The vendored files this differential does not replay, and why.
///
/// A file is either here or in `REPLAYED_FILES`; [`promql_corpus`] refuses to
/// build when the directory holds one that is in neither, so a newly vendored
/// file cannot arrive unnoticed and unrun.
const EXCLUDED_FILES: &[(&str, &str)] = &[(
    "limit.test",
    "limitk and limit_ratio live behind the promql crate's `experimental-functions` \
     feature, which the default build these suites link does not turn on",
)];

/// One series to seed into both engines.
#[derive(Clone, Debug, Default)]
pub struct CorpusSeries {
    /// Label pairs, `__name__` included, sorted by name.
    pub labels: Vec<(String, String)>,
    /// Float samples, ascending by timestamp.
    pub floats: Vec<(i64, f64)>,
    /// Native-histogram samples, ascending by timestamp.
    pub histograms: Vec<(i64, NativeHistogram)>,
}

/// How one case is evaluated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryKind {
    /// `/api/v1/query` at one millisecond timestamp.
    Instant {
        /// Evaluation timestamp, in milliseconds.
        time: i64,
    },
    /// `/api/v1/query_range` over a millisecond grid.
    Range {
        /// First step, in milliseconds.
        start: i64,
        /// Last step, in milliseconds.
        end: i64,
        /// Step width, in milliseconds.
        step: i64,
    },
}

/// One query put to both engines.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorpusCase {
    /// `<file>:<line>`, the line the `eval` statement begins on. A skip or
    /// divergence list names a case by this, and the name is something a reader
    /// can open the file at.
    pub name: String,
    /// The `PromQL` text, verbatim from the corpus.
    pub promql: String,
    /// Instant or range, with timestamps already shifted into the segment.
    pub kind: QueryKind,
    /// The corpus expects this query to be refused. Both engines have to refuse
    /// it and classify the refusal the same way, but the message itself is
    /// engine-specific prose and is not compared.
    pub expects_failure: bool,
}

/// One case the differential does not run, and why not.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkippedCase {
    /// The name the case would have had, or the file name for a whole file.
    pub name: String,
    /// The `PromQL` text, so a reader can find the case in the file.
    pub promql: String,
    /// Why it is not run. Never empty.
    pub reason: String,
}

/// The seed dataset and the case list the differential suites drive.
#[derive(Clone, Debug, Default)]
pub struct PromqlCorpus {
    /// Every series of every segment, on one timeline.
    pub series: Vec<CorpusSeries>,
    /// Cases to run, in corpus order.
    pub cases: Vec<CorpusCase>,
    /// Cases deliberately not run, each with a reason.
    pub skipped: Vec<SkippedCase>,
}

impl PromqlCorpus {
    /// The corpus with the named files' cases moved to the skip list.
    ///
    /// This is how a suite says that one upstream cannot be asked about a file
    /// at all -- an older `PromQL` that cannot parse the syntax the file is
    /// about, say. The samples stay seeded, because a file's series are not
    /// only its own.
    #[must_use]
    pub fn without_files(mut self, files: &[(&str, &str)]) -> Self {
        let mut kept = Vec::with_capacity(self.cases.len());
        for case in self.cases {
            match files
                .iter()
                .find(|(file, _)| case.name.starts_with(&format!("{file}:")))
            {
                Some((_, reason)) => self.skipped.push(SkippedCase {
                    name: case.name,
                    promql: case.promql,
                    reason: (*reason).to_string(),
                }),
                None => kept.push(case),
            }
        }
        self.cases = kept;
        self
    }

    /// The corpus with every custom-bucket histogram, and every case that
    /// names a metric carrying one, taken out.
    ///
    /// A `remote_write` receiver that refuses schema `-53` cannot be seeded
    /// with the corpus at all, so the samples have to go; the cases that read
    /// them would then compare two empty answers, which is worse than not
    /// running them, so they go too. A metric is matched by its name appearing
    /// in the query text as a whole token, which errs towards skipping a case
    /// that would have been fine.
    #[must_use]
    pub fn without_custom_bucket_histograms(mut self, reason: &str) -> Self {
        let mut dropped: BTreeSet<String> = BTreeSet::new();
        self.series.retain(|series| {
            if !series
                .histograms
                .iter()
                .any(|(_, histogram)| histogram.is_nhcb())
            {
                return true;
            }
            if let Some((_, name)) = series.labels.iter().find(|(name, _)| name == "__name__") {
                dropped.insert(name.clone());
            }
            false
        });

        let mut kept = Vec::with_capacity(self.cases.len());
        for case in self.cases {
            if dropped.iter().any(|name| names_metric(&case.promql, name)) {
                self.skipped.push(SkippedCase {
                    name: case.name,
                    promql: case.promql,
                    reason: reason.to_string(),
                });
            } else {
                kept.push(case);
            }
        }
        self.cases = kept;
        self
    }

    /// Total seeded samples, floats and histograms together.
    #[must_use]
    pub fn sample_count(&self) -> usize {
        self.series
            .iter()
            .map(|series| series.floats.len() + series.histograms.len())
            .sum()
    }
}

/// Reads the vendored corpus and lays it out on one timeline.
///
/// # Panics
///
/// Panics when the corpus directory is missing, when it holds a `.test` file
/// that neither `REPLAYED_FILES` nor `EXCLUDED_FILES` names, or when a file
/// fails to parse. All three mean the corpus and this module have drifted
/// apart, which is not something a differential run should paper over.
#[must_use]
pub fn promql_corpus() -> PromqlCorpus {
    let dir = corpus_dir();
    assert_corpus_files_are_accounted_for(&dir);

    let mut builder = CorpusBuilder::default();
    for file in REPLAYED_FILES {
        let path = dir.join(file);
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read `{}`: {error}", path.display()));
        let parsed = parse_test_file(&source)
            .unwrap_or_else(|error| panic!("parse `{}`: {error}", path.display()));
        builder.add_file(file, &parsed.statements, &eval_lines(&source));
    }
    for (file, reason) in EXCLUDED_FILES {
        builder.skipped.push(SkippedCase {
            name: (*file).to_string(),
            promql: String::new(),
            reason: (*reason).to_string(),
        });
    }
    builder.finish()
}

/// The line each `eval` statement of a `.test` file begins on.
///
/// The parser does not keep line numbers, and a case has to be nameable in a
/// skip or divergence list by something a reader can go and look at. Every eval
/// header in the corpus starts at column zero, so counting them in file order
/// lines them up with the statements the parser returns.
fn eval_lines(source: &str) -> Vec<usize> {
    source
        .lines()
        .enumerate()
        .filter(|(_, line)| line.starts_with("eval ") || line.starts_with("eval_fail "))
        .map(|(index, _)| index + 1)
        .collect()
}

/// The vendored corpus directory, found from wherever the suite runs.
///
/// Cargo runs an integration test from the crate directory, where the corpus is
/// a sibling crate's; Bazel runs it from the runfiles root, where a `data` file
/// keeps its own workspace path.
fn corpus_dir() -> PathBuf {
    ["../promql/tests/testdata", "crates/promql/tests/testdata"]
        .into_iter()
        .map(PathBuf::from)
        .find(|candidate| candidate.is_dir())
        .expect(
            "the vendored PromQL corpus is on the data path of the test. Bazel declares it in \
             //crates/metrics-service:BUILD.bazel; under Cargo it is read from the promql crate.",
        )
}

fn assert_corpus_files_are_accounted_for(dir: &Path) {
    let on_disk: BTreeSet<String> = std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read `{}`: {error}", dir.display()))
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            (path.extension()? == "test").then(|| path.file_name()?.to_str().map(str::to_string))?
        })
        .collect();
    let named: BTreeSet<String> = REPLAYED_FILES
        .iter()
        .map(|file| (*file).to_string())
        .chain(EXCLUDED_FILES.iter().map(|(file, _)| (*file).to_string()))
        .collect();
    assert!(
        on_disk == named,
        "the vendored corpus and this module disagree about which files exist. Every `.test` \
         file has to be replayed or excluded by name; see REPLAYED_FILES and EXCLUDED_FILES."
    );
}

/// One `eval` statement with the line it begins on.
struct NumberedEval<'a> {
    line: usize,
    statement: &'a Statement,
}

/// Accumulates segments onto one timeline as the files are walked.
#[derive(Default)]
struct CorpusBuilder {
    /// Series keyed by their `.test` selector text, so one metric written by
    /// several files stays one series on the timeline.
    series: BTreeMap<String, CorpusSeries>,
    cases: Vec<CorpusCase>,
    skipped: Vec<SkippedCase>,
    /// Where the next segment starts.
    offset_ms: i64,
}

impl CorpusBuilder {
    fn add_file(&mut self, file: &str, statements: &[Statement], eval_lines: &[usize]) {
        // Samples of every `load` seen since the last `clear`, keyed by the
        // series text and then by timestamp. A second `load` adds to this; a
        // series repeated *within* one `load` replaces the earlier line, which
        // is what upstream's `loadCmd` map does.
        let mut loaded: BTreeMap<String, BTreeMap<i64, SampleSpec>> = BTreeMap::new();
        let mut evals: Vec<NumberedEval<'_>> = Vec::new();
        let mut ordinal = 0_usize;

        for statement in statements {
            match statement {
                Statement::Clear => {
                    self.emit_segment(file, &loaded, &mut evals);
                    loaded.clear();
                }
                Statement::Load { step, series } => {
                    // A load after an eval is a new dataset for the evals that
                    // follow, so the ones before it are settled first.
                    self.emit_segment(file, &loaded, &mut evals);
                    apply_load(&mut loaded, step.millis_i64(), series);
                }
                Statement::EvalInstant { .. } | Statement::EvalRange { .. } => {
                    let line = *eval_lines.get(ordinal).unwrap_or_else(|| {
                        panic!("{file} has more eval statements than eval lines")
                    });
                    ordinal += 1;
                    evals.push(NumberedEval { line, statement });
                }
            }
        }
        self.emit_segment(file, &loaded, &mut evals);
    }

    /// Writes one segment's samples onto the timeline and turns its evals into
    /// cases, then advances the offset past both.
    fn emit_segment(
        &mut self,
        file: &str,
        loaded: &BTreeMap<String, BTreeMap<i64, SampleSpec>>,
        evals: &mut Vec<NumberedEval<'_>>,
    ) {
        if evals.is_empty() {
            return;
        }
        let offset = self.offset_ms;
        let mut span = 0_i64;

        for (metric, samples) in loaded {
            let series = self
                .series
                .entry(metric.clone())
                .or_insert_with(|| CorpusSeries {
                    labels: metric_to_labels(metric),
                    ..CorpusSeries::default()
                });
            for (timestamp, sample) in samples {
                span = span.max(*timestamp);
                let at = offset + timestamp;
                match sample {
                    SampleSpec::Value(value) => series.floats.push((at, *value)),
                    SampleSpec::Stale => series.floats.push((at, f64::from_bits(STALE_NAN_BITS))),
                    SampleSpec::Histogram(histogram) => {
                        series
                            .histograms
                            .push((at, merge_repeated_bounds(histogram)));
                    }
                    // `_` is a hole in the series, and a `load` cannot carry a
                    // string; `apply_load` dropped both already.
                    SampleSpec::Missing | SampleSpec::String(_) => {}
                }
            }
        }

        for eval in evals.drain(..) {
            let name = format!("{file}:{}", eval.line);
            let (case, divergence) = case_from_eval(name, eval.statement, offset);
            span = span.max(case_end_ms(case.kind) - offset);
            match skip_reason(&case, divergence, offset) {
                Some(reason) => self.skipped.push(SkippedCase {
                    name: case.name,
                    promql: case.promql,
                    reason,
                }),
                None => self.cases.push(case),
            }
        }

        self.offset_ms = offset + span + SEGMENT_GAP_MS;
    }

    fn finish(self) -> PromqlCorpus {
        let mut series: Vec<CorpusSeries> = self.series.into_values().collect();
        for one in &mut series {
            one.floats.sort_by_key(|(timestamp, _)| *timestamp);
            one.histograms.sort_by_key(|(timestamp, _)| *timestamp);
        }
        PromqlCorpus {
            series,
            cases: self.cases,
            skipped: self.skipped,
        }
    }
}

fn apply_load(
    loaded: &mut BTreeMap<String, BTreeMap<i64, SampleSpec>>,
    step_ms: i64,
    series: &[krabka_promql::LoadSeries],
) {
    let mut this_load: BTreeMap<String, Vec<(i64, SampleSpec)>> = BTreeMap::new();
    for load_series in series {
        let samples = load_series
            .values
            .iter()
            .enumerate()
            .filter(|(_, sample)| !matches!(sample, SampleSpec::Missing | SampleSpec::String(_)))
            .map(|(index, sample)| {
                let index = i64::try_from(index).expect("a sample index fits an i64");
                (index * step_ms, sample.clone())
            })
            .collect();
        // `insert` rather than `extend`: a series written twice in one `load`
        // keeps only the last line.
        this_load.insert(load_series.metric.clone(), samples);
    }
    for (metric, samples) in this_load {
        let entry = loaded.entry(metric).or_default();
        for (timestamp, sample) in samples {
            entry.insert(timestamp, sample);
        }
    }
}

fn case_from_eval(
    name: String,
    statement: &Statement,
    offset: i64,
) -> (CorpusCase, Option<String>) {
    match statement {
        Statement::EvalInstant {
            at_ms,
            expr,
            fail_message,
            divergence,
            ..
        } => (
            CorpusCase {
                name,
                promql: expr.clone(),
                kind: QueryKind::Instant {
                    time: offset + at_ms,
                },
                expects_failure: fail_message.is_some(),
            },
            divergence.clone(),
        ),
        Statement::EvalRange {
            start_ms,
            end_ms,
            step,
            expr,
            fail_message,
            divergence,
            ..
        } => (
            CorpusCase {
                name,
                promql: expr.clone(),
                kind: QueryKind::Range {
                    start: offset + start_ms,
                    end: offset + end_ms,
                    step: step.millis_i64(),
                },
                expects_failure: fail_message.is_some(),
            },
            divergence.clone(),
        ),
        Statement::Load { .. } | Statement::Clear => {
            unreachable!("only eval statements reach case_from_eval")
        }
    }
}

/// Why this case cannot be replayed against a container, when it cannot.
fn skip_reason(case: &CorpusCase, divergence: Option<String>, offset: i64) -> Option<String> {
    if let Some(reason) = divergence {
        return Some(format!("krabka:divergence {reason}"));
    }
    if offset != 0 && has_literal_at_modifier(&case.promql) {
        return Some(
            "the query pins an absolute `@` timestamp, which this segment's time shift would \
             move out from under it"
                .to_string(),
        );
    }
    None
}

/// Collapses a custom-bucket histogram's repeated upper bounds into one.
///
/// A `load_with_nhcb` block converts a classic histogram by reading the `le`
/// label of each bucket series, and `histograms.test` deliberately writes one
/// bound three ways -- `0.2`, `2e-1`, `2.0e-1`. Krabka's corpus loader keeps
/// those as three buckets sharing an upper bound, which no TSDB will accept:
/// Prometheus refuses a custom-bucket histogram whose bounds are not strictly
/// increasing. Upstream's own conversion keys buckets by their bound and so
/// never produces the shape at all, which is why the in-process conformance run
/// never sees this.
///
/// Merging them here is corpus preparation, not a comparison: both engines are
/// handed the merged histogram.
fn merge_repeated_bounds(histogram: &NativeHistogram) -> NativeHistogram {
    let mut merged = histogram.clone();
    let Some(bounds) = histogram.custom_values.as_deref() else {
        return merged;
    };
    if !histogram.is_nhcb() || bounds.windows(2).all(|pair| pair[0] < pair[1]) {
        return merged;
    }

    let dense = dense_buckets(
        &histogram.positive_spans,
        &histogram.positive_counts,
        bounds.len(),
    );
    let mut new_bounds: Vec<f64> = Vec::with_capacity(bounds.len());
    let mut new_counts: Vec<f64> = Vec::with_capacity(bounds.len() + 1);
    for (index, bound) in bounds.iter().enumerate() {
        let count = dense.get(index).copied().unwrap_or_default();
        if new_bounds.last() == Some(bound) {
            *new_counts
                .last_mut()
                .expect("a bucket accompanies every bound") += count;
        } else {
            new_bounds.push(*bound);
            new_counts.push(count);
        }
    }
    merged.custom_values = Some(new_bounds);
    if histogram.positive_counts.is_empty() {
        // No populated buckets to begin with: the bounds alone were invalid.
        return merged;
    }
    new_counts.push(dense.last().copied().unwrap_or_default());
    let length = u32::try_from(new_counts.len()).expect("a bucket count fits a u32");
    merged.positive_spans = vec![BucketSpan { offset: 0, length }];
    merged.positive_counts = new_counts;
    merged
}

/// Lays a histogram's spans out as one count per bucket index, gaps included.
fn dense_buckets(spans: &[BucketSpan], counts: &[f64], bounds: usize) -> Vec<f64> {
    let mut dense = vec![0.0; bounds + 1];
    let mut index = 0_i64;
    let mut counts = counts.iter();
    for span in spans {
        index += i64::from(span.offset);
        for _ in 0..span.length {
            if let Some(slot) = usize::try_from(index).ok().and_then(|at| dense.get_mut(at)) {
                *slot = counts.next().copied().unwrap_or_default();
            }
            index += 1;
        }
    }
    dense
}

fn case_end_ms(kind: QueryKind) -> i64 {
    match kind {
        QueryKind::Instant { time } => time,
        QueryKind::Range { end, .. } => end,
    }
}

/// Whether the expression uses `name` as a metric name rather than as part of
/// a longer one.
///
/// `metric` appears inside `some_metric`, and a plain substring test would take
/// every case about the second for a case about the first.
fn names_metric(expr: &str, name: &str) -> bool {
    let is_name_char = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b':';
    expr.match_indices(name).any(|(at, _)| {
        let before = at.checked_sub(1).map(|index| expr.as_bytes()[index]);
        let after = expr.as_bytes().get(at + name.len()).copied();
        !before.is_some_and(is_name_char) && !after.is_some_and(is_name_char)
    })
}

/// Whether the expression carries an `@` whose argument is a literal timestamp.
///
/// `@ start()` and `@ end()` move with the query and need no rewriting, so they
/// do not count. An `@` inside a string literal is not a modifier at all.
#[must_use]
pub fn has_literal_at_modifier(expr: &str) -> bool {
    let bytes = expr.as_bytes();
    let mut index = 0;
    let mut quote: Option<u8> = None;
    while index < bytes.len() {
        let byte = bytes[index];
        match quote {
            Some(open) => {
                if byte == b'\\' {
                    index += 2;
                    continue;
                }
                if byte == open {
                    quote = None;
                }
            }
            None => match byte {
                b'"' | b'\'' | b'`' => quote = Some(byte),
                b'@' => {
                    let rest = expr[index + 1..].trim_start();
                    if !rest.starts_with("start()") && !rest.starts_with("end()") {
                        return true;
                    }
                }
                _ => {}
            },
        }
        index += 1;
    }
    false
}

/// Splits `metric{label="value",…}` into label pairs, `__name__` included.
fn metric_to_labels(metric: &str) -> Vec<(String, String)> {
    let Some(open) = metric.find('{') else {
        return vec![("__name__".to_string(), metric.to_string())];
    };
    let mut labels = Vec::new();
    let name = &metric[..open];
    if !name.is_empty() {
        labels.push(("__name__".to_string(), name.to_string()));
    }
    let inside = metric[open + 1..].strip_suffix('}').unwrap_or_default();
    for pair in split_label_pairs(inside) {
        if let Some((key, value)) = pair.split_once('=') {
            labels.push((key.trim().to_string(), unquote_label_value(value.trim())));
        }
    }
    labels.sort_by(|left, right| left.0.cmp(&right.0));
    labels
}

fn split_label_pairs(inside: &str) -> Vec<&str> {
    let mut pairs = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    let mut escaped = false;
    for (index, ch) in inside.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_quotes => escaped = true,
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                pairs.push(inside[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    if start < inside.len() {
        pairs.push(inside[start..].trim());
    }
    pairs
}

fn unquote_label_value(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(value)
        .replace("\\\"", "\"")
        .replace("\\\\", "\\")
}

/// The widest time span one `remote_write` body may cover.
///
/// A TSDB head refuses a sample older than roughly an hour behind the newest
/// one it holds -- `head.appendableMinValidTime` is `MaxTime - chunkRange/2`,
/// and the default chunk range is two hours. The corpus lays its segments out
/// over weeks of simulated time, so writing it series by series pushes the head
/// to the last segment and then offers it the first one, which comes back as
/// `out of bounds`. Batches are therefore cut by time as well as by size, and
/// the samples inside one are within half an hour of each other.
const BATCH_WINDOW_MS: i64 = 30 * 60 * 1000;

/// The most samples one series may contribute to one `remote_write` body.
///
/// Krabka's distributor caps a request at ten thousand samples per series, and
/// `at_modifier.test` loads one series with 10001 of them a millisecond apart --
/// which is one batch's worth of a single half-hour window. Cutting the batch
/// here keeps the seed inside the limit without widening it for everyone.
const PER_SERIES_PER_BATCH: usize = 5_000;

/// One seeded sample, of either kind.
enum SeedSample<'a> {
    Float(f64),
    Histogram(&'a NativeHistogram),
}

/// Snappy-compressed `remote_write` v1 bodies carrying the whole seed.
///
/// The batches have to be posted in order. Both receivers hold one head per
/// tenant and neither accepts an append far behind it, so the seed is written in
/// ascending timestamp order across every series at once rather than one series
/// at a time.
///
/// # Panics
///
/// Panics when snappy compression fails, which it cannot for an in-memory
/// buffer.
#[must_use]
pub fn remote_write_batches(series: &[CorpusSeries], samples_per_batch: usize) -> Vec<Vec<u8>> {
    let mut ordered: Vec<(i64, usize, SeedSample<'_>)> = Vec::new();
    for (index, one) in series.iter().enumerate() {
        ordered.extend(
            one.floats
                .iter()
                .map(|(timestamp, value)| (*timestamp, index, SeedSample::Float(*value))),
        );
        ordered.extend(
            one.histograms.iter().map(|(timestamp, histogram)| {
                (*timestamp, index, SeedSample::Histogram(histogram))
            }),
        );
    }
    ordered.sort_by_key(|(timestamp, index, _)| (*timestamp, *index));

    let mut batches = Vec::new();
    let mut pending: BTreeMap<usize, Vec<(i64, SeedSample<'_>)>> = BTreeMap::new();
    let mut kinds: BTreeMap<usize, bool> = BTreeMap::new();
    let mut pending_count = 0_usize;
    let mut window_start = 0_i64;

    for (timestamp, index, sample) in ordered {
        let is_float = matches!(sample, SeedSample::Float(_));
        let over_size = pending_count >= samples_per_batch;
        let over_window = timestamp - window_start > BATCH_WINDOW_MS;
        let over_series = pending
            .get(&index)
            .is_some_and(|samples| samples.len() >= PER_SERIES_PER_BATCH);
        // One request may not carry the same series twice, once as floats and
        // once as histograms. Prometheus applies both entries in order; Mimir
        // 2.16.1 keeps only one of them and silently drops the other, so a
        // series that alternates between the two comes back missing the
        // histograms that sat between its floats.
        let changes_kind = kinds.get(&index).is_some_and(|last| *last != is_float);
        if !pending.is_empty() && (over_size || over_window || over_series || changes_kind) {
            batches.push(encode_batch(series, &std::mem::take(&mut pending)));
            kinds.clear();
            pending_count = 0;
        }
        if pending.is_empty() {
            window_start = timestamp;
        }
        kinds.insert(index, is_float);
        pending.entry(index).or_default().push((timestamp, sample));
        pending_count += 1;
    }
    if !pending.is_empty() {
        batches.push(encode_batch(series, &pending));
    }
    batches
}

fn encode_batch(
    series: &[CorpusSeries],
    pending: &BTreeMap<usize, Vec<(i64, SeedSample<'_>)>>,
) -> Vec<u8> {
    let timeseries = pending
        .iter()
        .flat_map(|(index, samples)| sample_runs(&series[*index].labels, samples))
        .collect();
    encode_write_request(timeseries)
}

fn encode_write_request(timeseries: Vec<pb::v1::TimeSeries>) -> Vec<u8> {
    let request = pb::v1::WriteRequest {
        timeseries,
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy compresses an in-memory remote_write body")
}

/// Cuts one series' share of a batch into runs of a single sample type.
///
/// A `remote_write` receiver appends a `TimeSeries`'s floats and then its
/// histograms, so a series that alternates between the two cannot travel as one
/// entry: the histograms would arrive behind floats that are already later in
/// time, and the append is refused. Entries are processed in request order, so
/// one entry per run keeps the whole series ascending.
fn sample_runs(
    labels: &[(String, String)],
    samples: &[(i64, SeedSample<'_>)],
) -> Vec<pb::v1::TimeSeries> {
    let labels = label_pairs(labels);
    let mut runs: Vec<pb::v1::TimeSeries> = Vec::new();
    let mut run_is_float: Option<bool> = None;
    for (timestamp, sample) in samples {
        let is_float = matches!(sample, SeedSample::Float(_));
        if run_is_float != Some(is_float) {
            runs.push(pb::v1::TimeSeries {
                labels: labels.clone(),
                ..Default::default()
            });
            run_is_float = Some(is_float);
        }
        let run = runs.last_mut().expect("a run was just pushed");
        match sample {
            SeedSample::Float(value) => run.samples.push(pb::v1::Sample {
                value: *value,
                timestamp: *timestamp,
            }),
            SeedSample::Histogram(histogram) => run
                .histograms
                .push(native_histogram_to_pb(histogram, *timestamp)),
        }
    }
    runs
}

fn label_pairs(labels: &[(String, String)]) -> Vec<pb::v1::Label> {
    labels
        .iter()
        .map(|(name, value)| pb::v1::Label {
            name: name.clone(),
            value: value.clone(),
        })
        .collect()
}

/// Re-encodes a corpus histogram as a `remote_write` v1 histogram.
///
/// The corpus carries absolute bucket counts; the wire format carries deltas for
/// an integer histogram and absolute floats for a float one, so this is the
/// inverse of what `krabka_metrics::wire` does on the way in.
fn native_histogram_to_pb(histogram: &NativeHistogram, timestamp: i64) -> pb::v1::Histogram {
    let mut out = pb::v1::Histogram {
        schema: i32::from(histogram.schema),
        sum: histogram.sum,
        zero_threshold: histogram.zero_threshold,
        positive_spans: spans_to_pb(&histogram.positive_spans),
        negative_spans: spans_to_pb(&histogram.negative_spans),
        reset_hint: reset_hint_to_pb(histogram.reset_hint),
        timestamp,
        custom_values: histogram.custom_values.clone().unwrap_or_default(),
        ..Default::default()
    };
    if histogram.is_float {
        out.count = Some(pb::v1::histogram::Count::CountFloat(histogram.count));
        out.zero_count = Some(pb::v1::histogram::ZeroCount::ZeroCountFloat(
            histogram.zero_count,
        ));
        out.positive_counts.clone_from(&histogram.positive_counts);
        out.negative_counts.clone_from(&histogram.negative_counts);
    } else {
        out.count = Some(pb::v1::histogram::Count::CountInt(whole_u64(
            histogram.count,
        )));
        out.zero_count = Some(pb::v1::histogram::ZeroCount::ZeroCountInt(whole_u64(
            histogram.zero_count,
        )));
        out.positive_deltas = counts_to_deltas(&histogram.positive_counts);
        out.negative_deltas = counts_to_deltas(&histogram.negative_counts);
    }
    out
}

fn spans_to_pb(spans: &[BucketSpan]) -> Vec<pb::v1::BucketSpan> {
    spans
        .iter()
        .map(|span| pb::v1::BucketSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect()
}

fn reset_hint_to_pb(hint: ResetHint) -> i32 {
    match hint {
        ResetHint::Unknown => pb::v1::histogram::ResetHint::Unknown as i32,
        ResetHint::Yes => pb::v1::histogram::ResetHint::Yes as i32,
        ResetHint::No => pb::v1::histogram::ResetHint::No as i32,
        ResetHint::Gauge => pb::v1::histogram::ResetHint::Gauge as i32,
    }
}

fn counts_to_deltas(counts: &[f64]) -> Vec<i64> {
    let mut previous = 0_i64;
    counts
        .iter()
        .map(|count| {
            let absolute = whole_i64(*count);
            let delta = absolute - previous;
            previous = absolute;
            delta
        })
        .collect()
}

/// The whole number an integer histogram's count holds.
///
/// `as` would do this in one instruction, but the cast lints are on by
/// workspace policy and suppressing one is not an option, so the value goes
/// through its own decimal rendering instead. That is exact for the whole
/// numbers an integer histogram carries, and it runs once per bucket while the
/// corpus is being built.
fn whole_i64(value: f64) -> i64 {
    format!("{value:.0}").parse().unwrap_or_default()
}

fn whole_u64(value: f64) -> u64 {
    format!("{value:.0}").parse().unwrap_or_default()
}

/// A behaviour on which Krabka and the upstream `PromQL` engine disagree, and
/// the corpus cases that show it.
///
/// The contract is the one `crates/promql/tests/testdata` uses for its
/// `# krabka:divergence` annotations, and it runs both ways: a listed case MUST
/// disagree, and an unlisted one MUST agree. A divergence that is later fixed
/// therefore cannot go on being listed as one, and a new disagreement cannot
/// hide behind a list that is merely long.
pub struct KnownDivergence {
    /// What Krabka does, what the upstream does, and why they differ. One
    /// entry per behaviour, not per case.
    pub reason: &'static str,
    /// The cases that show it, as `<file>:<line>`.
    pub cases: &'static [&'static str],
}

/// Where Krabka's engine disagrees with Prometheus's over this corpus.
///
/// Mimir embeds the same `PromQL` engine, so both differential suites start
/// from this list; `diff_mimir` adds the differences that are Mimir's own.
///
/// Nothing here is a comparison the differ was loosened to let through. Each
/// entry is a behaviour that was read off the two answers and understood, and
/// each is a bug against Krabka rather than a licence.
pub const UPSTREAM_DIVERGENCES: &[KnownDivergence] = &[
    KnownDivergence {
        reason: "An instant query stamps every sample with the timestamp of the sample it read. \
             Prometheus stamps it with the evaluation timestamp, whatever the sample's own is, so \
             the two agree only where a sample happens to sit exactly on the query time. Every \
             `@` case shows it, and so does any query whose lookback reaches back to an earlier \
             sample.",
        cases: &[
            "at_modifier.test:9",
            "at_modifier.test:13",
            "at_modifier.test:17",
            "at_modifier.test:21",
            "at_modifier.test:25",
            "at_modifier.test:29",
            "at_modifier.test:33",
            "at_modifier.test:37",
            "at_modifier.test:41",
            "at_modifier.test:45",
            "at_modifier.test:49",
            "at_modifier.test:53",
            "at_modifier.test:57",
            "at_modifier.test:61",
            "at_modifier.test:65",
            "at_modifier.test:70",
            "at_modifier.test:90",
            "duration_expression.test:116",
            "duration_expression.test:120",
            "duration_expression.test:123",
            "duration_expression.test:126",
            "duration_expression.test:129",
            "duration_expression.test:132",
            "duration_expression.test:135",
            "functions.test:1905",
            "functions.test:1909",
            "functions.test:1913",
            "functions.test:1917",
            "functions.test:1921",
            "functions.test:1925",
            "functions.test:1929",
            "functions.test:1933",
            "functions.test:1937",
            "functions.test:1941",
            "functions.test:1945",
            "functions.test:1949",
            "functions.test:1953",
            "functions.test:1957",
            "functions.test:1961",
            "functions.test:1965",
            "functions.test:1969",
            "functions.test:1973",
            "functions.test:1977",
            "functions.test:1981",
            "histograms.test:729",
            "histograms.test:737",
            "histograms.test:745",
            "native_histograms.test:5",
            "native_histograms.test:8",
            "native_histograms.test:11",
            "native_histograms.test:14",
            "native_histograms.test:17",
            "native_histograms.test:20",
            "native_histograms.test:30",
            "native_histograms.test:34",
            "native_histograms.test:38",
            "native_histograms.test:42",
            "native_histograms.test:46",
            "native_histograms.test:54",
            "native_histograms.test:190",
            "native_histograms.test:193",
            "native_histograms.test:196",
            "native_histograms.test:202",
            "native_histograms.test:206",
            "native_histograms.test:215",
            "native_histograms.test:218",
            "native_histograms.test:221",
            "native_histograms.test:225",
            "native_histograms.test:229",
            "native_histograms.test:562",
            "native_histograms.test:567",
            "native_histograms.test:570",
            "native_histograms.test:573",
            "native_histograms.test:583",
            "native_histograms.test:588",
            "native_histograms.test:591",
            "native_histograms.test:594",
            "native_histograms.test:604",
            "native_histograms.test:609",
            "native_histograms.test:612",
            "native_histograms.test:615",
            "native_histograms.test:624",
            "native_histograms.test:629",
            "native_histograms.test:632",
            "native_histograms.test:635",
            "native_histograms.test:1868",
            "native_histograms.test:1871",
            "operators.test:340",
            "operators.test:343",
            "operators.test:346",
            "operators.test:349",
            "operators.test:353",
            "operators.test:357",
            "operators.test:363",
            "operators.test:377",
            "operators.test:383",
            "operators.test:398",
            "operators.test:406",
            "operators.test:411",
            "operators.test:419",
            "operators.test:425",
            "operators.test:431",
            "operators.test:437",
            "operators.test:443",
            "operators.test:449",
            "operators.test:464",
            "operators.test:468",
            "operators.test:471",
            "operators.test:490",
            "operators.test:493",
            "operators.test:496",
            "operators.test:509",
            "operators.test:512",
            "selectors.test:117",
            "selectors.test:192",
            "staleness.test:13",
            "staleness.test:17",
            "staleness.test:52",
            "staleness.test:55",
            "trig_functions.test:8",
            "trig_functions.test:13",
            "trig_functions.test:18",
            "trig_functions.test:23",
            "trig_functions.test:28",
            "trig_functions.test:33",
            "trig_functions.test:38",
            "trig_functions.test:43",
            "trig_functions.test:48",
            "trig_functions.test:53",
            "trig_functions.test:58",
            "trig_functions.test:63",
            "trig_functions.test:68",
            "trig_functions.test:73",
            "trig_functions.test:78",
            "trig_functions.test:83",
            "trig_functions.test:88",
            "trig_functions.test:93",
        ],
    },
    KnownDivergence {
        reason: "Native-histogram JSON. Krabka renders `\"buckets\": []` where Prometheus omits the \
             field entirely, keeps buckets whose count is zero and a zero bucket whose count is \
             not positive where Prometheus drops both, and marks the lowest custom bucket's lower \
             bound exclusive where Prometheus marks `[-Inf, b]` closed.",
        cases: &[
            "functions.test:245",
            "functions.test:249",
            "functions.test:256",
            "functions.test:260",
            "functions.test:267",
            "functions.test:271",
            "functions.test:328",
            "functions.test:332",
            "functions.test:339",
            "functions.test:343",
            "functions.test:353",
            "functions.test:1075",
            "functions.test:1078",
            "functions.test:1081",
            "functions.test:1606",
            "functions.test:1618",
            "histograms.test:916",
            "histograms.test:920",
            "histograms.test:924",
            "histograms.test:1023",
            "histograms.test:1041",
            "histograms.test:1044",
            "histograms.test:1055",
            "native_histograms.test:5",
            "native_histograms.test:953",
            "native_histograms.test:957",
            "native_histograms.test:961",
            "native_histograms.test:981",
            "native_histograms.test:989",
            "native_histograms.test:993",
            "native_histograms.test:997",
            "native_histograms.test:1001",
            "native_histograms.test:1013",
            "native_histograms.test:1079",
            "native_histograms.test:1140",
            "native_histograms.test:1217",
            "native_histograms.test:1221",
            "native_histograms.test:1249",
            "native_histograms.test:1252",
            "native_histograms.test:1307",
            "native_histograms.test:1345",
            "native_histograms.test:1349",
            "native_histograms.test:1353",
            "native_histograms.test:1471",
            "native_histograms.test:1896",
            "native_histograms.test:1904",
            "native_histograms.test:1915",
            "operators.test:943",
            "operators.test:947",
            "range_queries.test:93",
            "subquery.test:146",
            "subquery.test:151",
        ],
    },
    KnownDivergence {
        reason: "A refused query is classified `bad_data` where Prometheus classifies it `execution`. \
             Both refuse it, and this compares only the classification -- the message itself is \
             engine-specific prose.",
        cases: &[
            "aggregators.test:422",
            "extended_vectors.test:364",
            "extended_vectors.test:367",
            "extended_vectors.test:370",
            "extended_vectors.test:373",
            "extended_vectors.test:376",
            "extended_vectors.test:379",
            "extended_vectors.test:386",
            "extended_vectors.test:389",
            "functions.test:519",
            "info.test:93",
        ],
    },
    KnownDivergence {
        reason: "A date function called with no argument returns a scalar. Prometheus returns an \
             instant vector holding one sample with no labels.",
        cases: &[
            "functions.test:1434",
            "functions.test:1446",
            "functions.test:1452",
            "functions.test:1458",
            "functions.test:1465",
            "functions.test:1471",
            "functions.test:1477",
        ],
    },
    KnownDivergence {
        reason: "`__type__` and `__unit__` survive arithmetic and stay on the result, where \
             Prometheus drops them; the labels that survive then change which series a set \
             operator pairs up. Krabka also renders `__unit__` with an empty value rather than \
             dropping the label.",
        cases: &[
            "type_and_unit.test:77",
            "type_and_unit.test:92",
            "type_and_unit.test:218",
            "type_and_unit.test:222",
            "type_and_unit.test:226",
            "type_and_unit.test:237",
        ],
    },
    KnownDivergence {
        reason: "A result that carries the same label set twice is answered rather than refused. \
             Prometheus fails the query with `vector cannot contain metrics with the same \
             labelset`.",
        cases: &["name_label_dropping.test:91"],
    },
];

/// Every case name the given divergence lists claim.
fn claimed_cases<'a>(lists: &[&'a [KnownDivergence]]) -> BTreeSet<&'a str> {
    lists
        .iter()
        .flat_map(|list| list.iter())
        .flat_map(|divergence| divergence.cases.iter().copied())
        .collect()
}

/// Compares a run's disagreements against the divergences it is allowed, and
/// describes the verdict when they do not line up.
///
/// `extra` names what is this upstream's own; `agreed` names shared divergences
/// this upstream does NOT show, which is how an upstream older than the corpus
/// says it still behaves the way Krabka does.
///
/// Three things are wrong here rather than one, and each is reported on its own
/// terms: a case that disagrees and is not listed, a listed case that has
/// stopped disagreeing, and a listed name that matches no case in the corpus at
/// all -- which is how a stale name survives a corpus edit.
#[must_use]
pub fn check_divergences(
    corpus: &PromqlCorpus,
    mismatches: &[(String, String)],
    extra: &[KnownDivergence],
    agreed: &[KnownDivergence],
) -> Option<String> {
    let listed = claimed_cases(&[UPSTREAM_DIVERGENCES, extra]);
    let agreed = claimed_cases(&[agreed]);
    let expected: BTreeSet<&str> = listed.difference(&agreed).copied().collect();
    let disagreed: BTreeSet<&str> = mismatches.iter().map(|(name, _)| name.as_str()).collect();
    let run: BTreeSet<&str> = corpus.cases.iter().map(|case| case.name.as_str()).collect();
    let known: BTreeSet<&str> = run
        .iter()
        .copied()
        .chain(corpus.skipped.iter().map(|case| case.name.as_str()))
        .collect();

    let unexpected: Vec<&str> = disagreed.difference(&expected).copied().collect();
    let settled: Vec<&str> = expected
        .difference(&disagreed)
        .copied()
        .filter(|name| run.contains(name))
        .collect();
    let stale: Vec<&str> = listed
        .union(&agreed)
        .copied()
        .filter(|name| !known.contains(name))
        .collect();
    if unexpected.is_empty() && settled.is_empty() && stale.is_empty() {
        return None;
    }

    let mut verdict = String::new();
    if !unexpected.is_empty() {
        let _ = writeln!(
            verdict,
            "{} case(s) disagree with the upstream and are not a known divergence:",
            unexpected.len()
        );
        for name in &unexpected {
            let detail = mismatches
                .iter()
                .find(|(case, _)| case == name)
                .map_or("", |(_, detail)| detail.as_str());
            let _ = writeln!(verdict, "  {detail}");
        }
    }
    if !settled.is_empty() {
        let _ = writeln!(
            verdict,
            "{} case(s) are listed as a known divergence but now agree with the upstream. \
             Remove them from the list rather than leaving it to describe a bug that is fixed:",
            settled.len()
        );
        for name in &settled {
            let _ = writeln!(verdict, "  {name}");
        }
    }
    if !stale.is_empty() {
        let _ = writeln!(
            verdict,
            "{} listed name(s) match no case in the corpus. The corpus moved underneath them:",
            stale.len()
        );
        for name in &stale {
            let _ = writeln!(verdict, "  {name}");
        }
    }
    Some(verdict)
}

/// Compares one case's two answers, and describes the difference if there is
/// one.
///
/// A case the corpus expects to fail is compared on `status` and `errorType`
/// alone: both engines have to refuse the query and classify the refusal the
/// same way, but the message is engine-specific prose and comparing it would
/// assert that Krabka reproduces Prometheus's wording.
#[must_use]
pub fn compare_case(case: &CorpusCase, krabka: &Value, upstream: &Value) -> Option<String> {
    let (left, right) = if case.expects_failure {
        (failure_shape(krabka), failure_shape(upstream))
    } else {
        (
            crate::diff_corpus::normalize(krabka),
            crate::diff_corpus::normalize(upstream),
        )
    };
    (left != right).then(|| {
        format!(
            "{} `{}` at {:?}\n      krabka:   {}\n      upstream: {}",
            case.name,
            case.promql,
            case.kind,
            compact(&left),
            compact(&right)
        )
    })
}

fn failure_shape(response: &Value) -> Value {
    serde_json::json!({
        "status": response.get("status").cloned().unwrap_or(Value::Null),
        "errorType": response.get("errorType").cloned().unwrap_or(Value::Null),
    })
}

/// How much of a mismatching response the report prints.
///
/// Six hundred characters is enough to see what differs; a triage run can widen
/// it with `KRABKA_DIFF_REPORT_WIDTH` when the difference is further in.
fn compact_limit() -> usize {
    std::env::var("KRABKA_DIFF_REPORT_WIDTH")
        .ok()
        .and_then(|width| width.parse().ok())
        .unwrap_or(600)
}

fn compact(value: &Value) -> String {
    let text = value.to_string();
    match text.char_indices().nth(compact_limit()) {
        Some((cut, _)) => format!("{}… ({} bytes)", &text[..cut], text.len()),
        None => text,
    }
}

/// Writes the run's report: what ran, what was skipped and why, which known
/// divergences showed themselves, and every disagreement in full.
///
/// A skipped case is only visible if something writes it down. This is the
/// counterpart of the `PromQL` conformance report.
///
/// # Panics
///
/// Panics when the report cannot be written, which would otherwise hide the
/// very list it exists to publish.
pub fn write_report(
    suite: &str,
    corpus: &PromqlCorpus,
    mismatches: &[(String, String)],
    extra: &[KnownDivergence],
) {
    let disagreed: BTreeSet<&str> = mismatches.iter().map(|(name, _)| name.as_str()).collect();
    let run: BTreeSet<&str> = corpus.cases.iter().map(|case| case.name.as_str()).collect();
    let mut report = String::new();
    let _ = writeln!(report, "{suite} differential report");
    let _ = writeln!(report, "seeded series: {}", corpus.series.len());
    let _ = writeln!(report, "seeded samples: {}", corpus.sample_count());
    let _ = writeln!(report, "cases run: {}", corpus.cases.len());
    let _ = writeln!(report, "cases skipped: {}", corpus.skipped.len());
    let _ = writeln!(report, "cases that disagreed: {}", mismatches.len());

    let _ = writeln!(report, "\nskipped:");
    for skipped in &corpus.skipped {
        let _ = writeln!(
            report,
            "  SKIP {} `{}`: {}",
            skipped.name, skipped.promql, skipped.reason
        );
    }

    let _ = writeln!(report, "\nknown divergences:");
    for divergence in UPSTREAM_DIVERGENCES.iter().chain(extra) {
        let showing = divergence
            .cases
            .iter()
            .filter(|name| disagreed.contains(*name))
            .count();
        let ran = divergence
            .cases
            .iter()
            .filter(|name| run.contains(*name))
            .count();
        let _ = writeln!(
            report,
            "  DIVERGENCE {showing}/{ran} cases run ({} listed): {}",
            divergence.cases.len(),
            divergence.reason
        );
        for name in divergence.cases {
            let mark = if disagreed.contains(name) {
                ""
            } else if run.contains(name) {
                " (AGREED)"
            } else {
                " (NOT RUN)"
            };
            let _ = writeln!(report, "    {name}{mark}");
        }
    }

    let _ = writeln!(report, "\ndisagreements:");
    for (_, detail) in mismatches {
        let _ = writeln!(report, "  MISMATCH {detail}");
    }

    let path = match std::env::var("TEST_UNDECLARED_OUTPUTS_DIR") {
        Ok(dir) => PathBuf::from(dir).join(format!("{suite}-report.txt")),
        Err(_) => PathBuf::from(format!("../../target/{suite}-report.txt")),
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create the differential report directory");
    }
    std::fs::write(&path, report).expect("write the differential report");
    println!("{suite}: report written to {}", path.display());
}
