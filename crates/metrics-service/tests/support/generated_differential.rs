//! Bounded, reproducible compositions checked against a live upstream oracle.

use std::{future::Future, path::Path};

use serde::Serialize;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const SEED: u64 = 42;

#[derive(Serialize)]
struct Attempt {
    expression: String,
    status: &'static str,
    details: Option<String>,
}

#[derive(Serialize)]
struct Case {
    expression: String,
    parents: Vec<String>,
    status: &'static str,
    details: Option<String>,
    reduced_expression: Option<String>,
    reduced_details: Option<String>,
    shrink_attempts: Vec<Attempt>,
}

fn next_index(state: &mut u64, bound: usize) -> usize {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    usize::try_from(*state % u64::try_from(bound).expect("bounded fixture count fits u64"))
        .expect("remainder fits usize")
}

fn case_count() -> TestResult<usize> {
    let maximum = match std::env::var("KRABKA_GENERATED_NIGHTLY") {
        Ok(value) if value == "1" => 512,
        Ok(value) if value == "0" => 64,
        Err(std::env::VarError::NotPresent) => 64,
        _ => return Err("KRABKA_GENERATED_NIGHTLY must be 0 or 1".into()),
    };
    let count = match std::env::var("KRABKA_GENERATED_CASES") {
        Ok(value) => value.parse::<usize>()?,
        Err(std::env::VarError::NotPresent) => 16,
        Err(error) => return Err(error.into()),
    };
    if count == 0 || count > maximum {
        return Err(format!("KRABKA_GENERATED_CASES must be in 1..={maximum}").into());
    }
    Ok(count)
}

fn write_report(language: &str, output: &Path, cases: &[Case]) -> TestResult {
    std::fs::create_dir_all(output)?;
    std::fs::write(
        output.join(format!("{language}-generated-differential.json")),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "language": language,
            "expected_outcome": if language.ends_with("-rejections") { "query-rejection" } else { "query-result" },
            "seed": SEED,
            "maximum_depth": 3,
            "planned": cases.len(),
            "passed": cases.iter().filter(|case| case.status == "pass").count(),
            "mismatched": cases.iter().filter(|case| case.status == "mismatch").count(),
            "transport_errors": cases.iter().filter(|case| case.status == "transport_error").count(),
            "shrink_transport_errors": cases.iter().flat_map(|case| &case.shrink_attempts).filter(|attempt| attempt.status == "transport_error").count(),
            "not_run": cases.iter().filter(|case| case.status == "not_run").count(),
            "cases": cases,
        }))?,
    )?;
    Ok(())
}

/// Generates seed-42 compositions with one to three unary wrappers per case.
///
/// The caller supplies valid, positive bases/templates and returns a mismatch
/// description for semantic disagreement. Transport failures abort the run;
/// every planned case and every attempted reduction remains in the report.
/// `KRABKA_GENERATED_CASES` defaults to 16 and is bounded by 64, or 512 when
/// `KRABKA_GENERATED_NIGHTLY=1`. Output paths are explicit to avoid environment
/// mutation in tests; Docker callers pass `TEST_UNDECLARED_OUTPUTS_DIR`.
///
/// # Errors
///
/// Returns an error for invalid inputs, transport or report failures, or any
/// semantic mismatch after writing the complete case report.
pub async fn run<F, Fut>(
    language: &str,
    bases: &[&str],
    templates: &[&str],
    output: &Path,
    compare: F,
) -> TestResult
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = TestResult<Option<String>>>,
{
    run_cases(language, bases, templates, output, case_count()?, compare).await
}

fn generated_cases(
    language: &str,
    bases: &[&str],
    templates: &[&str],
    count: usize,
) -> TestResult<Vec<Case>> {
    if language.is_empty()
        || !language
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        || bases.is_empty()
        || bases.iter().any(|base| base.trim().is_empty())
        || templates.is_empty()
        || templates
            .iter()
            .any(|template| template.matches("{expr}").count() != 1)
    {
        return Err(
            "generated differential needs a language, nonempty bases and unary templates".into(),
        );
    }
    let mut state = SEED;
    Ok((0..count)
        .map(|_| {
            let mut expression = bases[next_index(&mut state, bases.len())].to_owned();
            let mut parents = Vec::new();
            for _ in 0..=next_index(&mut state, 3) {
                parents.push(expression.clone());
                expression = templates[next_index(&mut state, templates.len())]
                    .replace("{expr}", &expression);
            }
            Case {
                expression,
                parents,
                status: "not_run",
                details: None,
                reduced_expression: None,
                reduced_details: None,
                shrink_attempts: Vec::new(),
            }
        })
        .collect())
}

async fn run_cases<F, Fut>(
    language: &str,
    bases: &[&str],
    templates: &[&str],
    output: &Path,
    count: usize,
    mut compare: F,
) -> TestResult
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = TestResult<Option<String>>>,
{
    let mut cases = generated_cases(language, bases, templates, count)?;
    for index in 0..cases.len() {
        let result = compare(cases[index].expression.clone()).await;
        let mismatch = match result {
            Ok(mismatch) => mismatch,
            Err(error) => {
                cases[index].status = "transport_error";
                cases[index].details = Some(error.to_string());
                write_report(language, output, &cases)?;
                return Err(error);
            }
        };
        let Some(details) = mismatch else {
            cases[index].status = "pass";
            continue;
        };
        cases[index].status = "mismatch";
        cases[index].details = Some(details.clone());
        let mut reduced_expression = cases[index].expression.clone();
        let mut reduced_details = details;
        for parent in cases[index].parents.clone().into_iter().rev() {
            if parent.len() >= reduced_expression.len() {
                continue;
            }
            match compare(parent.clone()).await {
                Ok(details) => {
                    cases[index].shrink_attempts.push(Attempt {
                        expression: parent.clone(),
                        status: if details.is_some() {
                            "mismatch"
                        } else {
                            "pass"
                        },
                        details: details.clone(),
                    });
                    if let Some(details) = details {
                        reduced_expression = parent;
                        reduced_details = details;
                    }
                }
                Err(error) => {
                    cases[index].shrink_attempts.push(Attempt {
                        expression: parent,
                        status: "transport_error",
                        details: Some(error.to_string()),
                    });
                    cases[index].reduced_expression = Some(reduced_expression);
                    cases[index].reduced_details = Some(reduced_details);
                    write_report(language, output, &cases)?;
                    return Err(error);
                }
            }
        }
        cases[index].reduced_expression = Some(reduced_expression);
        cases[index].reduced_details = Some(reduced_details);
    }
    write_report(language, output, &cases)?;
    let mismatches = cases
        .iter()
        .filter(|case| case.status == "mismatch")
        .count();
    if mismatches > 0 {
        return Err(format!(
            "{language}: {mismatches} generated mismatches; seed {SEED}; report in {}",
            output.display()
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[tokio::test]
    async fn a_known_semantic_mismatch_is_reduced_and_saved() {
        let output = tempfile::tempdir().unwrap();
        let result = run_cases(
            "promql",
            &["probe_metric"],
            &["abs({expr})"],
            output.path(),
            4,
            |expression| async move {
                Ok(expression
                    .starts_with("abs(")
                    .then(|| format!("oracle value differs for {expression}")))
            },
        )
        .await;
        assert!(result.is_err());
        let report: serde_json::Value = serde_json::from_slice(
            &std::fs::read(output.path().join("promql-generated-differential.json")).unwrap(),
        )
        .unwrap();
        assert!(report["seed"] == SEED);
        assert!(report["planned"] == 4);
        assert!(report["mismatched"] == 4);
        assert!(report["not_run"] == 0);
        let cases = report["cases"].as_array().unwrap();
        assert!(cases.len() == 4);
        assert!(
            cases
                .iter()
                .any(|case| case["expression"] != case["reduced_expression"])
        );
        for case in cases {
            assert!(case["reduced_expression"] == "abs(probe_metric)");
            assert!(case["reduced_details"] == "oracle value differs for abs(probe_metric)");
            let attempts = case["shrink_attempts"].as_array().unwrap();
            assert!(attempts.last().unwrap()["expression"] == "probe_metric");
            assert!(attempts.last().unwrap()["status"] == "pass");
        }
    }
}
