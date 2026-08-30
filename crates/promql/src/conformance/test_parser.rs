use super::{
    ExpectBlock, ExpectLine, Line, LoadSeries, Result, SampleSpec, Statement, TestFile, Time,
    TimeExt, failure_message, is_block_line, load_with_nhcb_series, parse_duration_ms, parse_error,
    parse_expect_directive, parse_expect_string, parse_range_vector_directive, parse_sample_token,
    split_metric_and_tail, split_once_whitespace, split_sample_tokens,
};

pub(crate) struct TestParser<'a> {
    pub(crate) lines: Vec<Line<'a>>,
    pub(crate) index: usize,
}

impl<'a> TestParser<'a> {
    pub(crate) fn new(src: &'a str) -> Self {
        let lines = src
            .lines()
            .enumerate()
            .filter_map(|(index, raw)| {
                let trimmed = raw.trim();
                (!trimmed.is_empty() && !trimmed.starts_with('#')).then_some(Line {
                    number: index + 1,
                    raw,
                    trimmed,
                })
            })
            .collect();
        Self { lines, index: 0 }
    }

    pub(crate) fn parse_file(&mut self) -> Result<TestFile> {
        let mut statements = Vec::new();

        while let Some(line) = self.peek() {
            if line.trimmed.starts_with("load ") {
                statements.push(self.parse_load(false)?);
            } else if line.trimmed.starts_with("load_with_nhcb ") {
                statements.push(self.parse_load(true)?);
            } else if line.trimmed == "clear" {
                self.index += 1;
                statements.push(Statement::Clear);
            } else if line.trimmed.starts_with("eval instant at ")
                || line.trimmed.starts_with("eval_fail instant at ")
            {
                statements.push(self.parse_eval_instant()?);
            } else if line.trimmed.starts_with("eval range from ")
                || line.trimmed.starts_with("eval_fail range from ")
            {
                statements.push(self.parse_eval_range()?);
            } else {
                return Err(parse_error(
                    line,
                    "expected load, eval, eval_fail, or clear",
                ));
            }
        }

        Ok(TestFile { statements })
    }

    pub(crate) fn parse_load(&mut self, with_nhcb: bool) -> Result<Statement> {
        let header = self.next().expect("peeked header");
        let step = if with_nhcb {
            header
                .trimmed
                .strip_prefix("load_with_nhcb ")
                .ok_or_else(|| parse_error(header, "expected load_with_nhcb statement"))?
        } else {
            header
                .trimmed
                .strip_prefix("load ")
                .ok_or_else(|| parse_error(header, "expected load statement"))?
        };
        let step = Time::from_millis(parse_duration_ms(step.trim(), header)?);
        let mut series = Vec::new();

        while let Some(line) = self.peek() {
            if !is_block_line(line) {
                break;
            }
            let line = self.next().expect("peeked block line");
            let (metric, values) = split_metric_and_tail(line.trimmed, line)?;
            let values = split_sample_tokens(values, line)?
                .into_iter()
                .map(|token| parse_sample_token(token, line))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect();
            series.push(LoadSeries {
                metric: metric.to_string(),
                values,
            });
        }

        if with_nhcb {
            series.extend(load_with_nhcb_series(&series, header)?);
        }

        Ok(Statement::Load { step, series })
    }

    pub(crate) fn parse_eval_instant(&mut self) -> Result<Statement> {
        let header = self.next().expect("peeked header");
        let (fail, rest) = if let Some(rest) = header.trimmed.strip_prefix("eval_fail instant at ")
        {
            (true, rest)
        } else {
            let rest = header
                .trimmed
                .strip_prefix("eval instant at ")
                .ok_or_else(|| parse_error(header, "expected instant eval statement"))?;
            (false, rest)
        };
        let (at, expr) = split_once_whitespace(rest.trim(), header)?;
        let ExpectBlock {
            lines: expect,
            annotations,
            fail_message: expect_fail_message,
            range,
        } = self.parse_expect_block()?;

        Ok(Statement::EvalInstant {
            at_ms: parse_duration_ms(at, header)?,
            expr: expr.to_string(),
            expect,
            annotations,
            range_expect: range,
            fail_message: failure_message(fail, expect_fail_message),
        })
    }

    pub(crate) fn parse_eval_range(&mut self) -> Result<Statement> {
        let header = self.next().expect("peeked header");
        let (fail, rest) = if let Some(rest) = header.trimmed.strip_prefix("eval_fail range from ")
        {
            (true, rest)
        } else {
            let rest = header
                .trimmed
                .strip_prefix("eval range from ")
                .ok_or_else(|| parse_error(header, "expected range eval statement"))?;
            (false, rest)
        };
        let (start, rest) = split_once_whitespace(rest.trim(), header)?;
        let rest = rest
            .strip_prefix("to ")
            .ok_or_else(|| parse_error(header, "expected `to` in range eval"))?;
        let (end, rest) = split_once_whitespace(rest.trim(), header)?;
        let rest = rest
            .strip_prefix("step ")
            .ok_or_else(|| parse_error(header, "expected `step` in range eval"))?;
        let (step, expr) = split_once_whitespace(rest.trim(), header)?;
        let ExpectBlock {
            lines: expect,
            annotations,
            fail_message: expect_fail_message,
            range,
        } = self.parse_expect_block()?;
        if range.is_some() {
            return Err(parse_error(
                header,
                "expect range vector is only valid for instant evals",
            ));
        }

        Ok(Statement::EvalRange {
            start_ms: parse_duration_ms(start, header)?,
            end_ms: parse_duration_ms(end, header)?,
            step: Time::from_millis(parse_duration_ms(step, header)?),
            expr: expr.to_string(),
            expect,
            annotations,
            fail_message: failure_message(fail, expect_fail_message),
        })
    }

    pub(crate) fn parse_expect_block(&mut self) -> Result<ExpectBlock> {
        let mut expect = Vec::new();
        let mut annotations = Vec::new();
        let mut fail_message = None;
        let mut range = None;

        while let Some(line) = self.peek() {
            if !is_block_line(line) {
                break;
            }
            let line = self.next().expect("peeked block line");
            if line.trimmed == "fail" {
                fail_message = Some(String::new());
                continue;
            }
            if let Some(directive) = line.trimmed.strip_prefix("expect ") {
                if directive == "fail" {
                    fail_message = Some(String::new());
                    continue;
                }
                if let Some(message) = directive.strip_prefix("fail msg:") {
                    fail_message = Some(message.trim().to_string());
                    continue;
                }
                if let Some(value) = directive.trim().strip_prefix("string ") {
                    expect.push(ExpectLine {
                        metric: String::new(),
                        values: vec![SampleSpec::String(parse_expect_string(value, line)?)],
                    });
                    continue;
                }
                if let Some(range_expect) = parse_range_vector_directive(directive, line)? {
                    if range.replace(range_expect).is_some() {
                        return Err(parse_error(line, "duplicate expect range vector directive"));
                    }
                    continue;
                }
                annotations.push(parse_expect_directive(directive, line)?);
                continue;
            }
            if !line.trimmed.contains(char::is_whitespace) {
                let values = parse_sample_token(line.trimmed, line)?
                    .into_iter()
                    .collect::<Vec<_>>();
                if !values.is_empty() {
                    expect.push(ExpectLine {
                        metric: String::new(),
                        values,
                    });
                    continue;
                }
            }
            let (metric, value) = split_metric_and_tail(line.trimmed, line)?;
            let values = split_sample_tokens(value, line)?
                .into_iter()
                .map(|token| parse_sample_token(token, line))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            if values.is_empty() {
                return Err(parse_error(line, "expected at least one expected value"));
            }
            expect.push(ExpectLine {
                metric: metric.to_string(),
                values,
            });
        }

        Ok(ExpectBlock {
            lines: expect,
            annotations,
            fail_message,
            range,
        })
    }

    pub(crate) fn peek(&self) -> Option<Line<'a>> {
        self.lines.get(self.index).copied()
    }

    pub(crate) fn next(&mut self) -> Option<Line<'a>> {
        let line = self.peek()?;
        self.index += 1;
        Some(line)
    }
}
