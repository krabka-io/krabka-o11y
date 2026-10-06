use super::{
    DurationExprContext, PromqlError, Result, Time, TimeExt, consume_ident, duration_unit_seconds,
    is_ident_start, ms_to_seconds, skip_ws,
};

pub(crate) struct DurationExprParser<'a> {
    pub(crate) chars: Vec<char>,
    pub(crate) index: usize,
    pub(crate) src: &'a str,
    pub(crate) context: DurationExprContext,
    node: serde_json::Value,
}

impl<'a> DurationExprParser<'a> {
    pub(crate) fn new(src: &'a str, context: DurationExprContext) -> Self {
        Self {
            chars: src.chars().collect(),
            index: 0,
            src,
            context,
            node: serde_json::Value::Null,
        }
    }

    pub(crate) fn parse(self) -> Result<f64> {
        self.parse_with_ast().map(|(value, _)| value)
    }

    pub(crate) fn parse_with_ast(mut self) -> Result<(f64, serde_json::Value)> {
        let value = self.parse_add_sub()?;
        self.skip_ws();
        if self.index != self.chars.len() {
            return Err(PromqlError::Parse(format!(
                "unexpected duration expression input `{}` in `{}`",
                self.chars[self.index], self.src
            )));
        }
        Ok((value, self.node))
    }

    fn binary_node(&mut self, op: &str, lhs: &serde_json::Value) {
        self.node = serde_json::json!({"type":"durationExpr", "op":op, "lhs":lhs, "rhs":self.node, "wrapped":false});
    }

    pub(crate) fn parse_add_sub(&mut self) -> Result<f64> {
        let mut value = self.parse_mul_div_mod()?;
        loop {
            self.skip_ws();
            let lhs = self.node.clone();
            if self.eat('+') {
                value += self.parse_mul_div_mod()?;
                self.binary_node("+", &lhs);
            } else if self.eat('-') {
                value -= self.parse_mul_div_mod()?;
                self.binary_node("-", &lhs);
            } else {
                return Ok(value);
            }
        }
    }

    pub(crate) fn parse_mul_div_mod(&mut self) -> Result<f64> {
        let mut value = self.parse_unary()?;
        loop {
            self.skip_ws();
            let lhs = self.node.clone();
            if self.eat('*') {
                value *= self.parse_unary()?;
                self.binary_node("*", &lhs);
            } else if self.eat('/') {
                let rhs = self.parse_unary()?;
                if self.node["type"] == "numberLiteral" && rhs == 0.0 {
                    return Err(PromqlError::Parse("division by zero".to_owned()));
                }
                value /= rhs;
                self.binary_node("/", &lhs);
            } else if self.eat('%') {
                let rhs = self.parse_unary()?;
                if self.node["type"] == "numberLiteral" && rhs == 0.0 {
                    return Err(PromqlError::Parse("modulo by zero".to_owned()));
                }
                value %= rhs;
                self.binary_node("%", &lhs);
            } else {
                return Ok(value);
            }
        }
    }

    pub(crate) fn parse_unary(&mut self) -> Result<f64> {
        self.skip_ws();
        if self.eat('+') {
            return self.parse_unary();
        }
        if self.eat('-') {
            let value = -self.parse_power()?;
            if self.node["type"] == "numberLiteral" {
                self.node["val"] = value.to_string().into();
            } else {
                self.binary_node("-", &serde_json::Value::Null);
            }
            return Ok(value);
        }
        self.parse_power()
    }

    pub(crate) fn parse_power(&mut self) -> Result<f64> {
        let base = self.parse_primary()?;
        self.skip_ws();
        if self.eat('^') {
            let lhs = self.node.clone();
            let value = base.powf(self.parse_unary()?);
            self.binary_node("^", &lhs);
            Ok(value)
        } else {
            Ok(base)
        }
    }

    pub(crate) fn parse_primary(&mut self) -> Result<f64> {
        self.skip_ws();
        if self.eat('(') {
            let value = self.parse_add_sub()?;
            self.skip_ws();
            if !self.eat(')') {
                return Err(PromqlError::Parse(format!(
                    "unclosed duration expression `{}`",
                    self.src
                )));
            }
            if self.node["type"] == "durationExpr" {
                self.node["wrapped"] = true.into();
            }
            return Ok(value);
        }
        if self.peek().is_some_and(is_ident_start) {
            return self.parse_call();
        }
        if self
            .peek()
            .is_some_and(|ch| ch.is_ascii_digit() || ch == '.')
        {
            return self.parse_number_or_duration();
        }
        Err(PromqlError::Parse(format!(
            "expected duration expression in `{}`",
            self.src
        )))
    }

    pub(crate) fn parse_call(&mut self) -> Result<f64> {
        let start = self.index;
        self.index = consume_ident(&self.chars, self.index);
        let name = self.chars[start..self.index].iter().collect::<String>();
        self.skip_ws();
        if !self.eat('(') {
            return Err(PromqlError::Parse(format!(
                "expected function call in duration expression `{}`",
                self.src
            )));
        }
        let mut args = Vec::new();
        let mut nodes = Vec::new();
        self.skip_ws();
        if !self.eat(')') {
            loop {
                args.push(self.parse_add_sub()?);
                nodes.push(self.node.clone());
                self.skip_ws();
                if self.eat(')') {
                    break;
                }
                if !self.eat(',') {
                    return Err(PromqlError::Parse(format!(
                        "expected `,` or `)` in duration expression `{}`",
                        self.src
                    )));
                }
            }
        }

        self.node = serde_json::json!({"type":"durationExpr", "op":name.to_ascii_lowercase(),
            "lhs":nodes.first(), "rhs":nodes.get(1), "wrapped":false});
        match name.to_ascii_lowercase().as_str() {
            "step" if args.is_empty() => Ok(self.context.step.secs_f64()),
            "range" if args.is_empty() => Ok(Time::from_millis(
                self.context.end_ms.saturating_sub(self.context.start_ms),
            )
            .secs_f64()),
            "start" if args.is_empty() => Ok(ms_to_seconds(self.context.start_ms)),
            "end" if args.is_empty() => Ok(ms_to_seconds(self.context.end_ms)),
            "min_of" if args.len() == 2 => Ok(if args[0].is_nan() || args[1].is_nan() {
                f64::NAN
            } else {
                args[0].min(args[1])
            }),
            "max_of" if args.len() == 2 => Ok(if args[0].is_nan() || args[1].is_nan() {
                f64::NAN
            } else {
                args[0].max(args[1])
            }),
            "min" if !args.is_empty() => Ok(args.into_iter().fold(f64::INFINITY, f64::min)),
            "max" if !args.is_empty() => Ok(args.into_iter().fold(f64::NEG_INFINITY, f64::max)),
            _ => Err(PromqlError::Parse(format!(
                "unsupported duration expression function `{name}`"
            ))),
        }
    }

    pub(crate) fn parse_number_or_duration(&mut self) -> Result<f64> {
        let start = self.index;
        let mut total = 0.0;
        let mut saw_unit = false;

        loop {
            let number_start = self.index;
            while self
                .peek()
                .is_some_and(|ch| ch.is_ascii_digit() || ch == '.')
            {
                self.index += 1;
            }
            if number_start == self.index {
                break;
            }
            let number = self.chars[number_start..self.index]
                .iter()
                .collect::<String>()
                .parse::<f64>()
                .map_err(|error| {
                    PromqlError::Parse(format!(
                        "invalid duration expression number `{}`: {error}",
                        self.chars[number_start..self.index]
                            .iter()
                            .collect::<String>()
                    ))
                })?;
            let unit_start = self.index;
            while self.peek().is_some_and(|ch| ch.is_ascii_alphabetic()) {
                self.index += 1;
            }
            if unit_start == self.index {
                if saw_unit {
                    self.index = number_start;
                    break;
                }
                validate_literal_duration(number)?;
                self.node = serde_json::json!({"type":"numberLiteral", "val":number.to_string(), "duration":false});
                return Ok(number);
            }
            saw_unit = true;
            total += number
                * duration_unit_seconds(
                    &self.chars[unit_start..self.index]
                        .iter()
                        .collect::<String>(),
                )?;
        }

        if saw_unit {
            validate_literal_duration(total)?;
            self.node = serde_json::json!({"type":"numberLiteral", "val":total.to_string(), "duration":true});
            Ok(total)
        } else {
            Err(PromqlError::Parse(format!(
                "expected number or duration in `{}`",
                self.chars[start..].iter().collect::<String>()
            )))
        }
    }

    pub(crate) fn skip_ws(&mut self) {
        self.index = skip_ws(&self.chars, self.index);
    }

    pub(crate) fn eat(&mut self, ch: char) -> bool {
        if self.peek() == Some(ch) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    pub(crate) fn peek(&self) -> Option<char> {
        self.chars.get(self.index).copied()
    }
}

// Go's time.Duration stores int64 nanoseconds, independently of our evaluator's
// millisecond grid. This is a literal parse bound, not a runtime folding bound.
fn validate_literal_duration(seconds: f64) -> Result<()> {
    if seconds > (2.0_f64.powi(63) / 1_000_000_000.0) {
        return Err(PromqlError::Parse("duration out of range".to_owned()));
    }
    Ok(())
}
