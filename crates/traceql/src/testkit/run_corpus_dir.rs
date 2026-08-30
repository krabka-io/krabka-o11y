use super::*;

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
