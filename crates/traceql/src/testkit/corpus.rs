use super::{Case, CaseResult, Path, Report, engine, file_name, fs, parse_field, run_case};

pub(crate) fn parse_cases(file: &str, contents: &str) -> Vec<Case> {
    contents
        .split("\n---")
        .enumerate()
        .filter_map(|entry| {
            let idx = entry.0;
            let block = entry.1;
            let mut case = Case {
                name: format!("{file}#{}", idx + 1),
                kind: "search".into(),
                ..Case::default()
            };
            for line in block.lines().map(str::trim) {
                if line.starts_with('#') {
                    continue;
                }
                let Some(parts) = line.split_once(':') else {
                    continue;
                };
                let key = parts.0;
                let value = parts.1.trim();
                match key.trim() {
                    "name" => case.name = format!("{file}:{value}"),
                    "kind" => case.kind = value.to_string(),
                    "query" => case.query = Some(value.to_string()),
                    "trace_id" => case.trace_id = Some(parse_field(&case.name, "trace_id", value)),
                    "expect_trace_ids" => case.expect_trace_ids = Some(value.to_string()),
                    "expect_span_ids" => case.expect_span_ids = Some(value.to_string()),
                    "expect_metrics" => case.expect_metrics = Some(value.to_string()),
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

/// # Panics
/// Panics if the current-thread Tokio runtime cannot be created.
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
