use super::*;

/// The corpus is chosen for the characters that have to survive two escaping
/// layers, or that betray one that is missing: quotes, backslashes, `LIKE`
/// wildcards, regex metacharacters, an embedded newline, and multi-byte text.
fn corpus() -> Vec<&'static str> {
    vec![
        "level=error msg=boom",
        "level=warn msg=boom",
        "100% cpu",
        "a_b_c",
        "path C:\\Users\\logs",
        "he said 'hi'",
        "err0r",
        "error",
        "panic:\ngoroutine 1",
        "поток error",
        "a.b",
        "axb",
        "start middle end",
        "start end",
        "endstart",
        "status 503",
        "",
    ]
}

async fn lines_selected_by_predicates(predicates: &[String]) -> Vec<String> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("line", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(corpus())) as ArrayRef],
    )
    .expect("corpus batch");
    let ctx = SessionContext::new();
    ctx.register_batch("logs", batch).expect("register corpus");

    let sql = format!("select line from logs where {}", predicates.join(" and "));
    let batches = ctx
        .sql(&sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} plans: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql} runs: {error}"));

    batches
        .iter()
        .flat_map(|batch| {
            let lines = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("line column is Utf8");
            (0..batch.num_rows())
                .map(|row| lines.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn lines_selected_by_pipeline(pipeline: &[PipelineStage]) -> Vec<String> {
    corpus()
        .into_iter()
        .filter(|line| pipeline.iter().all(|stage| stage.matches(line)))
        .map(str::to_string)
        .collect()
}

/// Each case is a query and the number of its line filters that reach SQL.
///
/// A case that pushes every one of its filters must select exactly the lines
/// the Rust pipeline selects. A case that pushes fewer may only select more:
/// the scan prunes, and the row-by-row pass decides.
#[tokio::test]
pub(crate) async fn a_pushed_line_filter_predicate_selects_the_same_lines_as_the_rust_filter() {
    let cases = [
        (r#"{app="a"} |= "error""#, 1),
        (r#"{app="a"} != "error""#, 1),
        (r#"{app="a"} |= "100%""#, 1),
        (r#"{app="a"} |= "a_b""#, 1),
        (r#"{app="a"} |= "C:\\Users""#, 1),
        (r#"{app="a"} |= "'hi'""#, 1),
        (r#"{app="a"} |~ "err(0|o)r""#, 1),
        (r#"{app="a"} !~ "err(0|o)r""#, 1),
        (r#"{app="a"} |~ "^level=error""#, 1),
        (r#"{app="a"} |~ "a.b""#, 1),
        (r#"{app="a"} |~ "a\\.b""#, 1),
        (r#"{app="a"} |~ "(?i)ERROR""#, 1),
        (r#"{app="a"} |~ "5\\d\\d""#, 1),
        (r#"{app="a"} |~ "100%""#, 1),
        (r#"{app="a"} |~ "'hi'""#, 1),
        (r#"{app="a"} |> "start<_>end""#, 1),
        (r#"{app="a"} !> "start<_>end""#, 1),
        (r#"{app="a"} |> "100%<_>cpu""#, 1),
        (r#"{app="a"} |= "level=" |~ "err(0|o)r" |> "level=<_>r""#, 3),
        // A regex whose literal text can hold a backslash is not pushed:
        // DataFusion would rewrite it into a LIKE that reads the backslash as
        // an escape and drops the lines it should keep.
        (r#"{app="a"} |~ "C:\\\\Users""#, 0),
        (r#"{app="a"} !~ "C:\\\\Users""#, 0),
        (r#"{app="a"} |~ "\\x41""#, 0),
        // An `ip(...)` filter and a `<_>`-only pattern have no SQL form.
        (r#"{app="a"} |= ip("127.0.0.1")"#, 0),
        (r#"{app="a"} |> "<_>""#, 0),
    ];

    for (query, expected_predicates) in cases {
        let parsed = parse_query(query).expect("query parses");
        let predicates = line_filter_sql_predicates(&parsed.pipeline);
        check!(predicates.len() == expected_predicates, "{query}");

        let rust = lines_selected_by_pipeline(&parsed.pipeline);
        if predicates.is_empty() {
            continue;
        }
        let pushed = lines_selected_by_predicates(&predicates).await;
        if predicates.len() == parsed.pipeline.len() {
            check!(pushed == rust, "{query}");
        } else {
            check!(rust.iter().all(|line| pushed.contains(line)), "{query}");
        }
    }
}
