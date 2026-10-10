use super::{CaseResult, Path, Report, engine, file_name, fs, parse_cases, run_case};

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

/// Run one corpus file with [`run_corpus_file`] and print the report's text.
///
/// # Errors
/// Returns the I/O error when `file` does not exist or cannot be read.
///
/// # Panics
/// Panics where [`run_corpus_file`] does.
pub fn run_and_print_corpus_file(file: &Path) -> std::io::Result<Report> {
    fs::metadata(file)?;
    let report = run_corpus_file(file);
    println!("{}", report.to_text());
    Ok(report)
}
