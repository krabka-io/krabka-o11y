//! File-backed `TraceQL` conformance testkit.

use std::{fmt::Write as _, fs, path::Path, sync::Arc};

use krabka_units::{Time, convert::TimeExt as _};

use crate::{AttrValue, EngineOpts, InMemorySpanStore, InputSpan, SearchResponse, TraceqlEngine};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaseResult {
    pub name: String,
    pub passed: bool,
    pub passed_assertions: usize,
    pub total_assertions: usize,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    pub cases: Vec<CaseResult>,
}

impl Report {
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn write_to(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, self.to_text())
    }

    #[must_use]
    pub fn to_text(&self) -> String {
        let passed = self.cases.iter().filter(|case| case.passed).count();
        let total = self.cases.len();
        let mut out = format!("TraceQL conformance: {passed}/{total} cases passed\n");
        for case in &self.cases {
            let status = if case.passed { "PASS" } else { "FAIL" };
            let _ = writeln!(
                out,
                "{status} {} ({}/{}) {}",
                case.name, case.passed_assertions, case.total_assertions, case.message
            );
        }
        out
    }
}

#[must_use]
/// # Panics
/// Panics if a parsed expression or span set violates an invariant established during `TraceQL` validation.
pub fn run_corpus_dir(dir: impl AsRef<Path>) -> Report {
    let dir = dir.as_ref();
    let mut files = match fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "case"))
            .collect::<Vec<_>>(),
        Err(err) => {
            return Report {
                cases: vec![CaseResult {
                    name: dir.display().to_string(),
                    passed: false,
                    passed_assertions: 0,
                    total_assertions: 1,
                    message: format!("failed to read corpus dir: {err}"),
                }],
            };
        }
    };
    files.sort();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("traceql conformance runtime");
    let engine = engine();
    let mut cases = Vec::new();
    for file in files {
        let rel = file_name(&file);
        match fs::read_to_string(&file) {
            Ok(contents) => {
                cases.extend(
                    parse_cases(&rel, &contents)
                        .into_iter()
                        .map(|case| rt.block_on(async { run_case(&engine, case).await })),
                );
            }
            Err(err) => cases.push(CaseResult {
                name: rel,
                passed: false,
                passed_assertions: 0,
                total_assertions: 1,
                message: format!("failed to read case file: {err}"),
            }),
        }
    }

    Report { cases }
}

#[must_use]
/// # Panics
/// Panics if a parsed expression or span set violates an invariant established during `TraceQL` validation.
pub fn run_corpus_file(file: impl AsRef<Path>) -> Report {
    let file = file.as_ref();
    let rel = file_name(file);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("traceql conformance runtime");
    let engine = engine();
    let cases = match fs::read_to_string(file) {
        Ok(contents) => parse_cases(&rel, &contents)
            .into_iter()
            .map(|case| rt.block_on(async { run_case(&engine, case).await }))
            .collect(),
        Err(err) => vec![CaseResult {
            name: rel,
            passed: false,
            passed_assertions: 0,
            total_assertions: 1,
            message: format!("failed to read case file: {err}"),
        }],
    };

    Report { cases }
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string()
}

async fn run_case(engine: &TraceqlEngine<InMemorySpanStore>, case: Case) -> CaseResult {
    match case.kind.as_str() {
        "search" => run_search_case(engine, case).await,
        "metrics" => run_metrics_case(engine, case).await,
        "trace_by_id" => run_trace_by_id_case(engine, case).await,
        other => CaseResult {
            name: case.name,
            passed: false,
            passed_assertions: 0,
            total_assertions: 1,
            message: format!("unknown case kind `{other}`"),
        },
    }
}

async fn run_search_case(engine: &TraceqlEngine<InMemorySpanStore>, case: Case) -> CaseResult {
    let mut result = CaseResult {
        name: case.name,
        passed: false,
        passed_assertions: 0,
        total_assertions: 2,
        message: String::new(),
    };
    let Some(query) = case.query else {
        result.message = "missing query".into();
        return result;
    };
    let response = match engine.search("t", &query, 0, 10_000, 20).await {
        Ok(response) => response,
        Err(err) => {
            result.message = err.to_string();
            return result;
        }
    };

    let expected_trace_ids = parse_u8_list(case.expect_trace_ids.as_deref());
    let actual_trace_ids = trace_ids(&response);
    if actual_trace_ids == expected_trace_ids {
        result.passed_assertions += 1;
    } else {
        let _ = write!(
            result.message,
            "trace ids expected {expected_trace_ids:?}, got {actual_trace_ids:?}; "
        );
    }

    let expected_span_ids = parse_u8_list(case.expect_span_ids.as_deref());
    let actual_span_ids = span_ids(&response);
    if actual_span_ids == expected_span_ids {
        result.passed_assertions += 1;
    } else {
        let _ = write!(
            result.message,
            "span ids expected {expected_span_ids:?}, got {actual_span_ids:?}; "
        );
    }

    result.passed = result.passed_assertions == result.total_assertions;
    result
}

async fn run_metrics_case(engine: &TraceqlEngine<InMemorySpanStore>, case: Case) -> CaseResult {
    let mut result = CaseResult {
        name: case.name,
        passed: false,
        passed_assertions: 0,
        total_assertions: 1,
        message: String::new(),
    };
    let Some(query) = case.query else {
        result.message = "missing query".into();
        return result;
    };
    let response = match engine.query_range("t", &query, 0, 10_000, 10_000).await {
        Ok(response) => response,
        Err(err) => {
            result.message = err.to_string();
            return result;
        }
    };
    let expected = case.expect_series_count.unwrap_or(0);
    let actual = response.series.len();
    if actual == expected {
        result.passed_assertions = 1;
        result.passed = true;
    } else {
        result.message = format!("series count expected {expected}, got {actual}");
    }
    result
}

async fn run_trace_by_id_case(engine: &TraceqlEngine<InMemorySpanStore>, case: Case) -> CaseResult {
    let mut result = CaseResult {
        name: case.name,
        passed: false,
        passed_assertions: 0,
        total_assertions: 1,
        message: String::new(),
    };
    let Some(trace_id) = case.trace_id else {
        result.message = "missing trace_id".into();
        return result;
    };
    let response = match engine.trace_by_id("t", &[trace_id; 16]).await {
        Ok(response) => response,
        Err(err) => {
            result.message = err.to_string();
            return result;
        }
    };
    let expected = case.expect_span_count.unwrap_or(0);
    let actual = response.map_or(0, |trace| trace.spans.len());
    if actual == expected {
        result.passed_assertions = 1;
        result.passed = true;
    } else {
        result.message = format!("span count expected {expected}, got {actual}");
    }
    result
}

#[derive(Debug, Default, PartialEq)]
struct Case {
    name: String,
    kind: String,
    query: Option<String>,
    trace_id: Option<u8>,
    expect_trace_ids: Option<String>,
    expect_span_ids: Option<String>,
    expect_series_count: Option<usize>,
    expect_span_count: Option<usize>,
}

fn parse_cases(file: &str, contents: &str) -> Vec<Case> {
    contents
        .split("\n---")
        .enumerate()
        .filter_map(|(idx, block)| {
            let mut case = Case {
                name: format!("{file}#{}", idx + 1),
                kind: "search".into(),
                ..Case::default()
            };
            for line in block.lines().map(str::trim) {
                if line.starts_with('#') {
                    continue;
                }
                let Some((key, value)) = line.split_once(':') else {
                    continue;
                };
                let value = value.trim();
                match key.trim() {
                    "name" => case.name = format!("{file}:{value}"),
                    "kind" => case.kind = value.to_string(),
                    "query" => case.query = Some(value.to_string()),
                    "trace_id" => case.trace_id = Some(parse_field(&case.name, "trace_id", value)),
                    "expect_trace_ids" => case.expect_trace_ids = Some(value.to_string()),
                    "expect_span_ids" => case.expect_span_ids = Some(value.to_string()),
                    "expect_series_count" => {
                        case.expect_series_count =
                            Some(parse_field(&case.name, "expect_series_count", value));
                    }
                    "expect_span_count" => {
                        case.expect_span_count =
                            Some(parse_field(&case.name, "expect_span_count", value));
                    }
                    // An unrecognised key is a typo in the corpus, not an
                    // optional extra. Silently ignoring it would drop the
                    // expectation it was meant to state and leave the case
                    // passing on a weaker assertion than its author wrote.
                    other => panic!("{}: unknown case key `{other}`", case.name),
                }
            }
            (!block.trim().is_empty()).then_some(case)
        })
        .collect()
}

/// Parses one numeric field of a corpus case, failing loudly rather than
/// leaving it unset. A field that will not parse is a mistake in the case
/// file, and treating it as absent removes the assertion it was written to
/// make.
fn parse_field<T: std::str::FromStr>(case: &str, key: &str, value: &str) -> T {
    value
        .parse()
        .unwrap_or_else(|_| panic!("{case}: `{key}` is not a valid value: {value:?}"))
}

/// Parses a comma-separated id list. Empty entries are allowed so a trailing
/// comma is harmless, but an entry that is not a number is a mistake in the
/// case file and stops the run: dropping it would silently shorten the list
/// the case is asserting against.
fn parse_u8_list(value: Option<&str>) -> Vec<u8> {
    value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            item.parse()
                .unwrap_or_else(|_| panic!("`{item}` is not a valid id in list {value:?}"))
        })
        .collect()
}

fn trace_ids(resp: &SearchResponse) -> Vec<u8> {
    resp.traces.iter().map(|trace| trace.trace_id[0]).collect()
}

fn span_ids(resp: &SearchResponse) -> Vec<u8> {
    let mut ids = resp
        .traces
        .iter()
        .flat_map(|trace| trace.span_sets.iter())
        .flat_map(|set| set.spans.iter())
        .map(|span| span.span_id[0])
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

fn engine() -> TraceqlEngine<InMemorySpanStore> {
    let mut store = InMemorySpanStore::new();
    store.push_trace(
        "t",
        "svc-a",
        "root-a",
        vec![
            span(
                1,
                1,
                None,
                "root-a",
                100,
                vec![
                    ("svc", AttrValue::Str("a".into())),
                    ("a", AttrValue::Int(1)),
                    ("http.method", AttrValue::Str("GET".into())),
                    ("name", AttrValue::Str("post-root".into())),
                ],
            ),
            span(
                1,
                2,
                Some(1),
                "child-x",
                200,
                vec![
                    ("svc", AttrValue::Str("b".into())),
                    ("b", AttrValue::Int(2)),
                ],
            ),
            span(
                1,
                4,
                Some(2),
                "grand-y",
                80,
                vec![("svc", AttrValue::Str("c".into()))],
            ),
            span(
                1,
                3,
                Some(1),
                "child-z",
                220,
                vec![("svc", AttrValue::Str("b".into()))],
            ),
        ],
    );
    store.push_trace(
        "t",
        "svc-x",
        "root-x",
        vec![span(
            2,
            1,
            None,
            "both",
            50,
            vec![
                ("svc", AttrValue::Str("x".into())),
                ("a", AttrValue::Int(1)),
                ("b", AttrValue::Int(2)),
                ("name", AttrValue::Str("xpost".into())),
            ],
        )],
    );
    store.push_trace(
        "t",
        "svc-d",
        "root-d",
        vec![
            span(
                3,
                1,
                None,
                "root-d",
                100,
                vec![("svc", AttrValue::Str("a".into()))],
            ),
            span(
                3,
                2,
                Some(1),
                "child-d",
                100,
                vec![("svc", AttrValue::Str("d".into()))],
            ),
        ],
    );
    TraceqlEngine::new(Arc::new(store), EngineOpts::default())
}

fn span(
    trace: u8,
    id: u8,
    parent: Option<u8>,
    name: &str,
    duration_nanos: i64,
    attrs: Vec<(&str, AttrValue)>,
) -> InputSpan {
    InputSpan {
        trace_id: [trace; 16],
        span_id: [id; 8],
        parent_span_id: parent.map(|p| [p; 8]),
        name: name.into(),
        kind: 0,
        start_unix_nano: 1_000 + i64::from(id),
        duration: Time::from_nanos(duration_nanos),
        status_code: 0,
        status_message: String::new(),
        instrumentation_name: String::new(),
        instrumentation_version: String::new(),
        attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        events: Vec::new(),
        links: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use assert2::assert;
    use krabka_units::nanos;

    use super::*;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("traceql-testkit-{name}-{unique}"))
    }

    #[test]
    fn report_text_and_write_include_case_statuses() {
        let report = Report {
            cases: vec![
                CaseResult {
                    name: "ok".into(),
                    passed: true,
                    passed_assertions: 1,
                    total_assertions: 1,
                    message: String::new(),
                },
                CaseResult {
                    name: "bad".into(),
                    passed: false,
                    passed_assertions: 1,
                    total_assertions: 2,
                    message: "mismatch".into(),
                },
            ],
        };

        let text = report.to_text();
        for needle in [
            "TraceQL conformance: 1/2 cases passed",
            "PASS ok (1/1)",
            "FAIL bad (1/2) mismatch",
        ] {
            assert!(text.contains(needle), "missing: {needle}");
        }

        let dir = temp_dir("report");
        let path = dir.join("nested").join("report.txt");
        report.write_to(&path).unwrap();
        let written = fs::read_to_string(&path).unwrap();
        assert!(written == text);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn run_corpus_dir_only_reads_case_files_by_file_name() {
        let dir = temp_dir("corpus");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("one.case"),
            r#"
name: explicit
query: { .svc = "a" }
expect_trace_ids: 1,3
"#,
        )
        .unwrap();
        fs::write(
            dir.join("ignored.txt"),
            r#"
name: ignored
query: { .svc = "x" }
"#,
        )
        .unwrap();

        let report = run_corpus_dir(&dir);

        assert!(report.cases.len() == 1);
        assert!(report.cases[0].name == "one.case:explicit");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn file_name_uses_only_last_path_component() {
        assert!(file_name(Path::new("nested/cases/selectors.case")) == "selectors.case");
    }

    #[test]
    fn span_helper_offsets_start_time_by_span_id() {
        let span = span(9, 2, Some(1), "child", 123, vec![]);

        assert!(
            span == InputSpan {
                trace_id: [9; 16],
                span_id: [2; 8],
                parent_span_id: Some([1; 8]),
                name: "child".into(),
                kind: 0,
                start_unix_nano: 1_002,
                duration: nanos(123),
                status_code: 0,
                status_message: String::new(),
                instrumentation_name: String::new(),
                instrumentation_version: String::new(),
                attrs: vec![],
                events: Vec::new(),
                links: Vec::new(),
            }
        );
    }

    /// `parse_u8_list` reads the id lists a case asserts against. Blank
    /// entries are tolerated so a trailing comma is harmless, but an entry
    /// that is not a number stops the run rather than shortening the list --
    /// a case asserting against three ids and silently checking two is worse
    /// than a case that fails.
    #[test]
    fn id_lists_tolerate_blanks_and_reject_nonsense() {
        let parse = super::parse_u8_list;

        assert!(parse(None) == Vec::<u8>::new(), "an absent list is empty");
        assert!(parse(Some("")) == Vec::<u8>::new());
        assert!(parse(Some("1,2,3")) == vec![1, 2, 3]);
        assert!(
            parse(Some(" 1 , 2 ")) == vec![1, 2],
            "space around entries is trimmed"
        );
        assert!(
            parse(Some("1,,2")) == vec![1, 2],
            "a blank entry is skipped"
        );
        assert!(parse(Some("1,2,")) == vec![1, 2], "so is a trailing comma");
        assert!(parse(Some("7")) == vec![7], "a single id needs no comma");
        assert!(parse(Some("0,255")) == vec![0, 255], "the full byte range");
    }

    /// `run_search_case` is what turns a corpus case into a verdict, so a
    /// fault here weakens every case at once rather than one of them. It is
    /// exercised against the same engine the corpus uses, with a case known
    /// to hold and the same case with each expectation spoiled in turn.
    #[tokio::test]
    async fn a_search_case_passes_only_when_both_expectations_hold() {
        let engine = super::engine();
        let case = |traces: &str, spans: &str| super::Case {
            name: "t".into(),
            kind: "search".into(),
            query: Some(r#"{ .http.method = "GET" }"#.into()),
            trace_id: None,
            expect_trace_ids: Some(traces.into()),
            expect_span_ids: Some(spans.into()),
            expect_series_count: None,
            expect_span_count: None,
        };

        let result = super::run_search_case(&engine, case("1", "1")).await;
        assert!(result.passed, "message: {}", result.message);
        assert!(result.passed_assertions == 2);
        assert!(result.total_assertions == 2);

        // Each expectation on its own must be able to fail the case.
        let result = super::run_search_case(&engine, case("2", "1")).await;
        assert!(!result.passed, "a wrong trace id must fail");
        assert!(
            result.passed_assertions == 1,
            "the span assertion still held"
        );
        assert!(result.message.contains("trace ids expected"));

        let result = super::run_search_case(&engine, case("1", "2")).await;
        assert!(!result.passed, "a wrong span id must fail");
        assert!(
            result.passed_assertions == 1,
            "the trace assertion still held"
        );
        assert!(result.message.contains("span ids expected"));

        let result = super::run_search_case(&engine, case("2", "2")).await;
        assert!(!result.passed);
        assert!(result.passed_assertions == 0, "neither assertion held");

        // A case with no query asserts nothing and must not be reported as a
        // pass, which is the vacuous outcome worth guarding against.
        let mut empty = case("1", "1");
        empty.query = None;
        let result = super::run_search_case(&engine, empty).await;
        assert!(!result.passed);
        assert!(result.message == "missing query");
    }

    /// A search reports how much span data it scanned, which Tempo surfaces
    /// as `metrics.inspectedBytes`. It is accumulated across the scans a
    /// selector plans, so a query that matches nothing still inspects
    /// something, and a query over more data inspects at least as much.
    #[tokio::test]
    async fn a_search_reports_the_span_data_it_scanned() {
        use krabka_units::convert::ByteSizeExt;

        let engine = super::engine();
        let inspected = |query: &'static str| async move {
            super::engine()
                .search("t", query, 0, 10_000, 20)
                .await
                .expect("query runs")
                .inspected
        };

        let matched = inspected(r#"{ .http.method = "GET" }"#).await;
        assert!(
            matched > <krabka_units::ByteSize as ByteSizeExt>::ZERO,
            "a search that scans spans reports the bytes it read, got {matched:?}"
        );

        // A query matching nothing still had to read the spans to find that
        // out, so the figure reflects scanning rather than results.
        let unmatched = inspected(r#"{ .http.method = "NOPE" }"#).await;
        assert!(
            unmatched > <krabka_units::ByteSize as ByteSizeExt>::ZERO,
            "a search that matches nothing still scans, got {unmatched:?}"
        );

        let response = engine
            .search("t", r#"{ .http.method = "GET" }"#, 0, 10_000, 20)
            .await
            .expect("query runs");
        assert!(
            response.inspected == matched,
            "the figure is stable across runs"
        );
    }

    /// A selector over a nested scope with more than one alternative is
    /// planned as separate scans and their results merged. Nothing reached
    /// that path before: the shared fixture carries no events, so every query
    /// took the single-scan branch.
    ///
    /// The bytes each scan inspected have to be summed across them, which is
    /// what the response reports as Tempo's `metrics.inspectedBytes`.
    #[tokio::test]
    async fn a_disjunct_selector_sums_what_each_scan_inspected() {
        use krabka_units::{ByteSize, Time, convert::ByteSizeExt};

        use crate::{in_memory::InMemorySpanStore, result::EventRef};

        let with_event = |id: u8, event: &str| {
            let mut input = super::span(1, id, None, "root", 100, vec![]);
            input.events = vec![EventRef {
                time_since_start: Time::from_nanos(1),
                name: event.to_string(),
                attributes: vec![],
            }];
            input
        };

        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![with_event(1, "alpha"), with_event(2, "beta")],
        );
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());

        // Two alternatives over a nested scope: this is the disjunct path.
        let response = engine
            .search(
                "t",
                r#"{ event:name = "alpha" || event:name = "beta" }"#,
                0,
                10_000,
                20,
            )
            .await
            .expect("query runs");

        assert!(
            response.inspected > <ByteSize as ByteSizeExt>::ZERO,
            "the scanned bytes are summed across disjuncts, got {:?}",
            response.inspected
        );
        assert!(
            !response.traces.is_empty(),
            "both alternatives match a span"
        );
    }

    /// The metrics and trace-by-id runners each carry a single assertion, so
    /// the only thing standing between a real check and a vacuous pass is
    /// that one comparison. Both are exercised with a case that holds and the
    /// same case with the expectation moved off by one.
    #[tokio::test]
    async fn the_single_assertion_runners_can_fail_their_case() {
        let engine = super::engine();

        let metrics_case = |count: usize| super::Case {
            name: "t".into(),
            kind: "metrics".into(),
            query: Some("{ .svc != nil } | rate()".into()),
            trace_id: None,
            expect_trace_ids: None,
            expect_span_ids: None,
            expect_series_count: Some(count),
            expect_span_count: None,
        };

        let result = super::run_metrics_case(&engine, metrics_case(1)).await;
        assert!(result.passed, "message: {}", result.message);
        assert!(result.passed_assertions == 1 && result.total_assertions == 1);

        let result = super::run_metrics_case(&engine, metrics_case(2)).await;
        assert!(!result.passed, "a wrong series count must fail");
        assert!(result.passed_assertions == 0);
        assert!(result.message.contains("series count expected 2, got 1"));

        // A metrics case that states no series count is asserting zero, not
        // opting out. This query yields one, so it must fail.
        let mut unstated = metrics_case(1);
        unstated.expect_series_count = None;
        let result = super::run_metrics_case(&engine, unstated).await;
        assert!(!result.passed, "an unstated count means zero");
        assert!(result.message.contains("series count expected 0, got 1"));

        let mut no_query = metrics_case(1);
        no_query.query = None;
        let result = super::run_metrics_case(&engine, no_query).await;
        assert!(!result.passed && result.message == "missing query");

        let by_id_case = |trace_id, count: usize| super::Case {
            name: "t".into(),
            kind: "trace_by_id".into(),
            query: None,
            trace_id,
            expect_trace_ids: None,
            expect_span_ids: None,
            expect_series_count: None,
            expect_span_count: Some(count),
        };

        let result = super::run_trace_by_id_case(&engine, by_id_case(Some(1), 4)).await;
        assert!(result.passed, "message: {}", result.message);

        let result = super::run_trace_by_id_case(&engine, by_id_case(Some(1), 3)).await;
        assert!(!result.passed, "a wrong span count must fail");
        assert!(result.message.contains("span count expected 3, got 4"));

        // A trace that is not there has no spans, which a case may assert.
        let result = super::run_trace_by_id_case(&engine, by_id_case(Some(9), 0)).await;
        assert!(result.passed, "message: {}", result.message);

        // Unstated means zero here too, which an absent trace satisfies.
        let mut unstated = by_id_case(Some(9), 4);
        unstated.expect_span_count = None;
        let result = super::run_trace_by_id_case(&engine, unstated).await;
        assert!(result.passed, "message: {}", result.message);

        let result = super::run_trace_by_id_case(&engine, by_id_case(None, 4)).await;
        assert!(!result.passed && result.message == "missing trace_id");
    }

    #[test]
    #[should_panic(expected = "`x` is not a valid id")]
    fn an_unparseable_id_stops_the_run() {
        super::parse_u8_list(Some("1,x,3"));
    }

    #[test]
    #[should_panic(expected = "`256` is not a valid id")]
    fn an_id_outside_a_byte_stops_the_run() {
        super::parse_u8_list(Some("256"));
    }

    #[test]
    #[should_panic(expected = "unknown case key `expect_span_counts`")]
    fn a_misspelled_case_key_stops_the_run() {
        super::parse_cases("f.case", "name: a\nexpect_span_counts: 4\n");
    }

    #[test]
    #[should_panic(expected = "`expect_span_count` is not a valid value")]
    fn a_malformed_case_number_stops_the_run() {
        super::parse_cases("f.case", "name: a\nexpect_span_count: four\n");
    }

    /// The two id collectors differ deliberately: trace ids keep the order
    /// the engine returned them in, because that order is part of what a
    /// search case asserts, while span ids are sorted because they are
    /// gathered across span sets whose order is not meaningful.
    #[test]
    fn trace_ids_keep_engine_order_and_span_ids_are_sorted() {
        use krabka_units::{bytes, millis};

        use crate::result::{SearchResponse, SpanRef, SpanSet, TraceResult};

        // Only the first byte of each id is read, but the ids are fixed-size,
        // so both are widened from the byte under test.
        let span = |id: u8| SpanRef {
            span_id: [id, 0, 0, 0, 0, 0, 0, 0],
            parent_span_id: None,
            name: String::new(),
            kind: 0,
            nested_set_left: 0,
            nested_set_right: 0,
            nested_set_parent: 0,
            start_time_unix_nano: 0,
            duration: millis(0),
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            resource_attributes: vec![],
            attributes: vec![],
            events: vec![],
            links: vec![],
        };
        let trace = |id: u8, spans: Vec<u8>| TraceResult {
            trace_id: [id, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            root_service_name: String::new(),
            root_trace_name: String::new(),
            start_time_unix_nano: 0,
            duration: millis(0),
            span_sets: vec![SpanSet {
                spans: spans.into_iter().map(span).collect(),
                matched: 0,
            }],
        };
        let resp = SearchResponse {
            traces: vec![trace(3, vec![9, 1]), trace(1, vec![5])],
            inspected_traces: 0,
            inspected: bytes(0),
        };

        assert!(
            super::trace_ids(&resp) == vec![3, 1],
            "engine order is kept"
        );
        assert!(
            super::span_ids(&resp) == vec![1, 5, 9],
            "span ids are sorted"
        );
    }

    #[test]
    fn parse_cases_defaults_name_and_kind_for_unnamed_search_blocks() {
        let cases = parse_cases(
            "selectors.case",
            r#"
query: { .svc = "a" }
expect_trace_ids: 1
expect_span_ids: 1
"#,
        );

        assert!(
            cases
                == vec![Case {
                    name: "selectors.case#1".into(),
                    kind: "search".into(),
                    query: Some(r#"{ .svc = "a" }"#.into()),
                    trace_id: None,
                    expect_trace_ids: Some("1".into()),
                    expect_span_ids: Some("1".into()),
                    expect_series_count: None,
                    expect_span_count: None,
                }]
        );
    }

    #[test]
    fn parse_cases_splits_blocks_and_applies_explicit_names() {
        let cases = parse_cases(
            "selectors.case",
            r#"
# name: ignored
name: first
query: { .svc = "a" }

---

name: second
kind: metrics
query: { .svc = "b" } | count_over_time()
expect_series_count: 1
"#,
        );

        assert!(
            cases
                == vec![
                    Case {
                        name: "selectors.case:first".into(),
                        kind: "search".into(),
                        query: Some(r#"{ .svc = "a" }"#.into()),
                        trace_id: None,
                        expect_trace_ids: None,
                        expect_span_ids: None,
                        expect_series_count: None,
                        expect_span_count: None,
                    },
                    Case {
                        name: "selectors.case:second".into(),
                        kind: "metrics".into(),
                        query: Some(r#"{ .svc = "b" } | count_over_time()"#.into()),
                        trace_id: None,
                        expect_trace_ids: None,
                        expect_span_ids: None,
                        expect_series_count: Some(1),
                        expect_span_count: None,
                    },
                ]
        );
    }
}
