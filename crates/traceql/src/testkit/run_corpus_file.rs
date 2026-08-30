use super::*;

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
