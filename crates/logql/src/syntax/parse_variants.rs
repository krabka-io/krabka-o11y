use super::{
    LogqlExpr, ParseError, Parser, PipelineStage, function_args, parse_expr, scan_top_level,
    split_metric_parentheses, syntax_error,
};

pub(crate) fn parse_variants(input: &str) -> Result<Option<LogqlExpr>, ParseError> {
    let Some(rest) = input.strip_prefix("variants") else {
        return Ok(None);
    };
    if !rest.trim_start().starts_with('(') {
        return Ok(None);
    }
    let mut boundary = None;
    scan_top_level(input, |at| {
        if input[at..].starts_with("of") && input[..at].trim_end().ends_with(')') {
            boundary = Some(at);
        }
    })?;
    let at = boundary.ok_or_else(|| syntax_error("expected of(log range) after variants"))?;
    let args = function_args(input[..at].trim_end(), "variants")?
        .ok_or_else(|| syntax_error("expected variant expressions"))?;
    let common = function_args(&input[at..], "of")?
        .filter(|args| args.len() == 1)
        .ok_or_else(|| syntax_error("expected one common log range"))?;
    let variants = args
        .into_iter()
        .map(|text| {
            let expression = parse_expr(text)?;
            if matches!(expression, LogqlExpr::Stream { .. }) || expression.contains_variants() {
                return Err(syntax_error("expected metric variant"));
            }
            Ok(expression)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let common_text = if let Some((inner, rest)) = split_metric_parentheses(common[0]) {
        format!("{inner}{rest}")
    } else {
        common[0].to_string()
    };
    let mut parser = Parser::new(&common_text);
    let (mut stream, range_ns, offset_ns) = parser.parse_metric_range_stream_query()?;
    if parser.pos != parser.input.len() {
        return Err(syntax_error("expected end of common log range"));
    }
    // The common extractor takes LogRange.Left.Pipeline(); unwrap and its
    // postfilters belong to LogRange.Unwrap, so they are not common stages.
    if let Some(unwrap) = stream
        .pipeline
        .iter()
        .position(|stage| matches!(stage, PipelineStage::Unwrap(_)))
    {
        stream.pipeline.truncate(unwrap);
    }
    Ok(Some(LogqlExpr::Variants {
        variants,
        stream,
        range_ns,
        offset_ns,
        source: input.to_string(),
    }))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::parse_logql_expr;

    #[test]
    fn variants_parse_and_round_trip_balanced_pipeline_literals() {
        let source = r#"variants(count_over_time({app="ignored"}[1m]), sum by (app)(rate({app="ignored"}[30s]))) of ({app="api"} |= "of(a,b)" [2m] offset 10s)"#;
        let expression = parse_logql_expr(source).unwrap();
        assert!(expression.to_string() == source);
        let super::LogqlExpr::Variants {
            variants,
            range_ns,
            offset_ns,
            stream,
            ..
        } = expression
        else {
            panic!("expected variants")
        };
        assert!(variants.len() == 2);
        assert!(range_ns.0 == 120_000_000_000);
        assert!(offset_ns.0 == 10_000_000_000);
        assert!(stream.pipeline.len() == 1);
        assert!(
            parse_logql_expr(
                r#"variants(count_over_time({app="ignored"}[1m])) of (({app="api"} |= "ok")[2m])"#
            )
            .is_ok()
        );
        for invalid in [
            "variants() of ({app=\"api\"}[1m])",
            "sum(variants(count_over_time({app=\"api\"}[1m])) of ({app=\"api\"}[1m]))",
            "variants({app=\"api\"}) of ({app=\"api\"}[1m])",
            "variants(count_over_time({app=\"api\"}[1m]))",
            "variants(count_over_time({app=\"api\"}[1m])) of ({app=\"api\"})",
        ] {
            assert!(parse_logql_expr(invalid).is_err());
        }
    }
}
