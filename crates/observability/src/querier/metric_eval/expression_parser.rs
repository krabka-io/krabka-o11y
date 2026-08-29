use super::*;

impl<'a> VectorScalarExpressionParser<'a> {
    pub(crate) fn new(input: &'a str) -> Self {
        Self {
            input,
            position: 0,
            vector_terms: 0,
        }
    }

    pub(crate) fn parse_result(&mut self) -> Option<ScalarVectorExpressionResult> {
        if self.input[self.position..].starts_with("label_replace(") {
            return self.parse_label_replace_result();
        }
        if self.input[self.position..].starts_with("label_join(") {
            return self.parse_label_join_result();
        }

        let left_vector_terms = self.vector_terms;
        let left = self.parse_expression()?;
        let left_contains_vector = self.vector_terms > left_vector_terms;
        if let Some(operator) = self.parse_set_operator() {
            let right_vector_terms = self.vector_terms;
            let _right = self.parse_expression()?;
            let right_contains_vector = self.vector_terms > right_vector_terms;
            if !left_contains_vector || !right_contains_vector {
                return None;
            }
            let sample = match operator {
                ScalarSetOp::And | ScalarSetOp::Or => Some(left.format()),
                ScalarSetOp::Unless => None,
            };
            return Some(ScalarVectorExpressionResult::Vector {
                sample,
                metric: BTreeMap::new(),
            });
        }

        let Some(operator) = self.parse_comparison_operator() else {
            return Some(if self.vector_terms > 0 {
                ScalarVectorExpressionResult::Vector {
                    sample: Some(left.format()),
                    metric: BTreeMap::new(),
                }
            } else {
                ScalarVectorExpressionResult::Scalar {
                    sample: left.format(),
                }
            });
        };

        let bool_modifier = self.consume_keyword("bool");
        let left_vector_terms = self.vector_terms;
        let has_matching_modifier = self.consume_vector_matching_modifier()?;
        let right_vector_terms = self.vector_terms;
        let right = self.parse_expression()?;
        self.validate_vector_matching_modifier(
            has_matching_modifier,
            left_vector_terms,
            right_vector_terms,
        )?;
        let comparison_matches = left.compare(operator, right)?;
        if self.vector_terms == 0 {
            if bool_modifier {
                return None;
            }
            return Some(ScalarVectorExpressionResult::Scalar {
                sample: if comparison_matches { "1" } else { "0" }.to_string(),
            });
        }
        let sample = if bool_modifier {
            Some(if comparison_matches { "1" } else { "0" }.to_string())
        } else if comparison_matches {
            Some(left.format())
        } else {
            None
        };
        Some(ScalarVectorExpressionResult::Vector {
            sample,
            metric: BTreeMap::new(),
        })
    }

    pub(crate) fn parse_label_replace_result(&mut self) -> Option<ScalarVectorExpressionResult> {
        self.consume_keyword("label_replace");
        self.consume('(').then_some(())?;
        let result = self.parse_result()?;
        self.consume(',').then_some(())?;
        let destination_label = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let replacement = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let source_label = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let pattern = self.parse_string_literal()?;
        self.consume(')').then_some(())?;

        let ScalarVectorExpressionResult::Vector { sample, mut metric } = result else {
            return None;
        };
        let regex = Regex::new(&pattern).ok()?;
        let source_value = metric.get(&source_label).map_or("", String::as_str);
        if let Some(captures) = regex.captures(source_value) {
            let mut destination_value = String::new();
            captures.expand(&replacement, &mut destination_value);
            metric.insert(destination_label, destination_value);
        }

        Some(ScalarVectorExpressionResult::Vector { sample, metric })
    }

    pub(crate) fn parse_label_join_result(&mut self) -> Option<ScalarVectorExpressionResult> {
        self.consume_keyword("label_join");
        self.consume('(').then_some(())?;
        let result = self.parse_result()?;
        self.consume(',').then_some(())?;
        let destination_label = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let separator = self.parse_string_literal()?;
        self.consume(',').then_some(())?;
        let mut source_labels = vec![self.parse_string_literal()?];
        while self.consume(',') {
            source_labels.push(self.parse_string_literal()?);
        }
        self.consume(')').then_some(())?;

        let ScalarVectorExpressionResult::Vector { sample, mut metric } = result else {
            return None;
        };
        let joined = source_labels
            .iter()
            .map(|label| metric.get(label).map_or("", String::as_str))
            .collect::<Vec<_>>()
            .join(&separator);
        metric.insert(destination_label, joined);

        Some(ScalarVectorExpressionResult::Vector { sample, metric })
    }

    pub(crate) fn parse_expression(&mut self) -> Option<ScalarSample> {
        let mut sample = self.parse_product()?;
        loop {
            if self.consume('+') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_product()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.add(right)?;
            } else if self.consume('-') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_product()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.subtract(right)?;
            } else {
                return Some(sample);
            }
        }
    }

    pub(crate) fn parse_product(&mut self) -> Option<ScalarSample> {
        let mut sample = self.parse_power()?;
        loop {
            if self.consume('*') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_power()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.multiply(right)?;
            } else if self.consume('/') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_power()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.divide(right)?;
            } else if self.consume('%') {
                let left_vector_terms = self.vector_terms;
                let has_matching_modifier = self.consume_vector_matching_modifier()?;
                let right_vector_terms = self.vector_terms;
                let right = self.parse_power()?;
                self.validate_vector_matching_modifier(
                    has_matching_modifier,
                    left_vector_terms,
                    right_vector_terms,
                )?;
                sample = sample.modulo(right)?;
            } else {
                return Some(sample);
            }
        }
    }

    pub(crate) fn parse_power(&mut self) -> Option<ScalarSample> {
        let sample = self.parse_primary()?;
        if self.consume('^') {
            let left_vector_terms = self.vector_terms;
            let has_matching_modifier = self.consume_vector_matching_modifier()?;
            let right_vector_terms = self.vector_terms;
            let right = self.parse_power()?;
            self.validate_vector_matching_modifier(
                has_matching_modifier,
                left_vector_terms,
                right_vector_terms,
            )?;
            sample.power(right)
        } else {
            Some(sample)
        }
    }

    pub(crate) fn parse_primary(&mut self) -> Option<ScalarSample> {
        if self.consume('(') {
            let sample = self.parse_expression()?;
            return self.consume(')').then_some(sample);
        }

        self.parse_vector_scalar()
            .or_else(|| self.parse_scalar_literal())
    }

    pub(crate) fn parse_comparison_operator(&mut self) -> Option<ScalarComparisonOp> {
        for (operator, op) in [
            (">=", ScalarComparisonOp::GreaterOrEqual),
            ("<=", ScalarComparisonOp::LessOrEqual),
            ("==", ScalarComparisonOp::Equal),
            ("!=", ScalarComparisonOp::NotEqual),
            (">", ScalarComparisonOp::Greater),
            ("<", ScalarComparisonOp::Less),
        ] {
            if self.input[self.position..].starts_with(operator) {
                self.position += operator.len();
                return Some(op);
            }
        }
        None
    }

    pub(crate) fn parse_set_operator(&mut self) -> Option<ScalarSetOp> {
        for (operator, op) in [
            ("unless", ScalarSetOp::Unless),
            ("and", ScalarSetOp::And),
            ("or", ScalarSetOp::Or),
        ] {
            if self.input[self.position..].starts_with(operator) {
                self.position += operator.len();
                return Some(op);
            }
        }
        None
    }

    pub(crate) fn consume_vector_matching_modifier(&mut self) -> Option<bool> {
        if self.consume_keyword("on") || self.consume_keyword("ignoring") {
            self.consume_label_list()?;
            self.consume_group_modifier()?;
            Some(true)
        } else {
            Some(false)
        }
    }

    pub(crate) fn consume_group_modifier(&mut self) -> Option<()> {
        if !(self.consume_keyword("group_left") || self.consume_keyword("group_right")) {
            return Some(());
        }
        if self.input[self.position..].starts_with('(') {
            self.consume_label_list()?;
        }
        Some(())
    }

    pub(crate) fn consume_label_list(&mut self) -> Option<()> {
        self.consume('(').then_some(())?;
        if self.consume(')') {
            return Some(());
        }

        loop {
            self.consume_label_name()?;
            if self.consume(')') {
                return Some(());
            }
            self.consume(',').then_some(())?;
        }
    }

    pub(crate) fn consume_label_name(&mut self) -> Option<()> {
        let bytes = self.input.as_bytes();
        let first = *bytes.get(self.position)?;
        if !matches!(first, b'A'..=b'Z' | b'a'..=b'z' | b'_') {
            return None;
        }

        // `+= 1` against `*= 1` is a permanent survivor here. The first byte
        // has just been checked to be a letter or an underscore, and the loop
        // below accepts exactly those plus digits -- so leaving the position
        // on it lets the loop step over it instead, and the name ends in the
        // same place either way.
        self.position += 1;
        while matches!(
            bytes.get(self.position),
            Some(b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_')
        ) {
            self.position += 1;
        }
        Some(())
    }

    pub(crate) fn validate_vector_matching_modifier(
        &self,
        has_matching_modifier: bool,
        left_vector_terms: usize,
        right_vector_terms: usize,
    ) -> Option<()> {
        if !has_matching_modifier {
            return Some(());
        }

        let left_contains_vector = left_vector_terms > 0;
        let right_contains_vector = self.vector_terms > right_vector_terms;
        (left_contains_vector && right_contains_vector).then_some(())
    }

    pub(crate) fn parse_vector_scalar(&mut self) -> Option<ScalarSample> {
        let rest = &self.input[self.position..];
        let scalar = rest.strip_prefix("vector(")?;
        let scalar_end = scalar.find(')')?;
        let scalar_text = &scalar[..scalar_end];
        if scalar_text.starts_with(['+', '-']) {
            return None;
        }
        self.position += "vector(".len() + scalar_end + 1;
        let sample = parse_scalar_sample(scalar_text)?;
        self.vector_terms += 1;
        Some(sample)
    }

    pub(crate) fn parse_scalar_literal(&mut self) -> Option<ScalarSample> {
        let rest = &self.input[self.position..];
        let literal_len = scalar_literal_len(rest)?;
        let sample = parse_scalar_sample(&rest[..literal_len])?;
        self.position += literal_len;
        Some(sample)
    }

    pub(crate) fn parse_string_literal(&mut self) -> Option<String> {
        self.consume('"').then_some(())?;
        let mut value = String::new();
        // `<` is a permanent mutation survivor against `<=`: the extra pass it
        // allows slices an empty remainder, whose first character is `None`,
        // and the `?` returns the same `None` the loop would have fallen to.
        while self.position < self.input.len() {
            let ch = self.input[self.position..].chars().next()?;
            self.position += ch.len_utf8();
            match ch {
                '"' => return Some(value),
                '\\' => {
                    let escaped = self.input[self.position..].chars().next()?;
                    self.position += escaped.len_utf8();
                    value.push(match escaped {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        '"' => '"',
                        '\\' => '\\',
                        other => other,
                    });
                }
                other => value.push(other),
            }
        }
        None
    }

    pub(crate) fn consume(&mut self, operator: char) -> bool {
        if self.input[self.position..].starts_with(operator) {
            self.position += operator.len_utf8();
            true
        } else {
            false
        }
    }

    pub(crate) fn consume_keyword(&mut self, keyword: &str) -> bool {
        if self.input[self.position..].starts_with(keyword) {
            self.position += keyword.len();
            true
        } else {
            false
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.position == self.input.len()
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ScalarSetOp {
    And,
    Or,
    Unless,
}

pub(crate) fn scalar_literal_len(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut position = 0;
    if matches!(bytes.get(position), Some(b'+' | b'-')) {
        position += 1;
    }

    let whole_start = position;
    while matches!(bytes.get(position), Some(byte) if byte.is_ascii_digit()) {
        position += 1;
    }
    let whole_digits = position > whole_start;

    let mut fractional_digits = false;
    if matches!(bytes.get(position), Some(b'.')) {
        position += 1;
        let fractional_start = position;
        while matches!(bytes.get(position), Some(byte) if byte.is_ascii_digit()) {
            position += 1;
        }
        fractional_digits = position > fractional_start;
    }

    if !whole_digits && !fractional_digits {
        return None;
    }

    if matches!(bytes.get(position), Some(b'e' | b'E')) {
        position += 1;
        if matches!(bytes.get(position), Some(b'+' | b'-')) {
            position += 1;
        }
        let exponent_start = position;
        while matches!(bytes.get(position), Some(byte) if byte.is_ascii_digit()) {
            position += 1;
        }
        if position == exponent_start {
            return None;
        }
    }

    Some(position)
}

#[derive(Clone, Copy)]
pub(crate) enum ScalarComparisonOp {
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}
