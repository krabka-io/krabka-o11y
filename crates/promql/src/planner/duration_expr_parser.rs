use super::*;

pub(crate) struct DurationExprParser<'a> {
    pub(crate) chars: Vec<char>,
    pub(crate) index: usize,
    pub(crate) src: &'a str,
    pub(crate) context: DurationExprContext,
}

impl<'a> DurationExprParser<'a> {
    pub(crate) fn new(src: &'a str, context: DurationExprContext) -> Self {
        Self {
            chars: src.chars().collect(),
            index: 0,
            src,
            context,
        }
    }

    pub(crate) fn parse(mut self) -> Result<f64> {
        let value = self.parse_add_sub()?;
        self.skip_ws();
        if self.index != self.chars.len() {
            return Err(PromqlError::Parse(format!(
                "unexpected duration expression input `{}` in `{}`",
                self.chars[self.index], self.src
            )));
        }
        Ok(value)
    }

    pub(crate) fn parse_add_sub(&mut self) -> Result<f64> {
        let mut value = self.parse_mul_div_mod()?;
        loop {
            self.skip_ws();
            if self.eat('+') {
                value += self.parse_mul_div_mod()?;
            } else if self.eat('-') {
                value -= self.parse_mul_div_mod()?;
            } else {
                return Ok(value);
            }
        }
    }

    pub(crate) fn parse_mul_div_mod(&mut self) -> Result<f64> {
        let mut value = self.parse_unary()?;
        loop {
            self.skip_ws();
            if self.eat('*') {
                value *= self.parse_unary()?;
            } else if self.eat('/') {
                value /= self.parse_unary()?;
            } else if self.eat('%') {
                value %= self.parse_unary()?;
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
            return Ok(-self.parse_power()?);
        }
        self.parse_power()
    }

    pub(crate) fn parse_power(&mut self) -> Result<f64> {
        let base = self.parse_primary()?;
        self.skip_ws();
        if self.eat('^') {
            Ok(base.powf(self.parse_unary()?))
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
        self.skip_ws();
        if !self.eat(')') {
            loop {
                args.push(self.parse_add_sub()?);
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

        match name.to_ascii_lowercase().as_str() {
            "step" if args.is_empty() => Ok(self.context.step.secs_f64()),
            "range" if args.is_empty() => Ok(Time::from_millis(
                self.context.end_ms.saturating_sub(self.context.start_ms),
            )
            .secs_f64()),
            "start" if args.is_empty() => Ok(ms_to_seconds(self.context.start_ms)),
            "end" if args.is_empty() => Ok(ms_to_seconds(self.context.end_ms)),
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
            Ok(total)
        } else {
            Err(PromqlError::Parse(format!(
                "expected number or duration in `{}`",
                &self.src[start..]
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
