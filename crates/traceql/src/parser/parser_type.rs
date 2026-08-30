use super::{
    Aggregate, ComparisonOp, DEFAULT_COMPARE_TOP_N, Field, FieldExpr, Intrinsic, Pipeline, Query,
    QueryHints, Result, Scope, SpansetExpr, StructuralOp, Token, TraceqlError, Value, WithBinding,
    intrinsic, is_duration_field, numeric_filter_field, numeric_filter_value, parse_duration_nanos,
    scope, scopeless_intrinsic, value_add, value_div, value_mod, value_mul, value_neg, value_pow,
    value_sub,
};

pub(crate) struct Parser {
    pub(crate) tokens: Vec<Token>,
    pub(crate) pos: usize,
}

impl Parser {
    pub(crate) fn parse_query(&mut self) -> Result<Query> {
        let root = self.parse_spanset_or()?;
        let pipeline = self.parse_pipeline()?;
        let hints = self.parse_query_hints()?;
        self.expect(&Token::Eof)?;
        Ok(Query {
            root,
            pipeline,
            hints,
        })
    }

    pub(crate) fn parse_query_hints(&mut self) -> Result<QueryHints> {
        if !matches!(self.peek(), Token::Ident(name) if name == "with") {
            return Ok(QueryHints::default());
        }
        self.pos += 1;
        self.expect(&Token::LParen)?;
        let mut hints = QueryHints::default();
        loop {
            let name = self.expect_ident()?;
            self.expect(&Token::Eq)?;
            let value = match self.advance() {
                Token::Bool(value) => value,
                other => {
                    return Err(Self::err(format!(
                        "expected boolean query hint value, got {other:?}"
                    )));
                }
            };
            match name.as_str() {
                "most_recent" => hints.most_recent = value,
                "exemplars" => hints.exemplars = Some(value),
                "sample" => hints.sample = Some(value),
                other => return Err(Self::err(format!("unsupported query hint {other:?}"))),
            }
            if !eat!(self, &Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RParen)?;
        Ok(hints)
    }

    pub(crate) fn parse_pipeline(&mut self) -> Result<Vec<Pipeline>> {
        let mut out = Vec::new();
        while eat!(self, &Token::Pipe) {
            out.push(self.parse_pipeline_stage()?);
            if let Some(by) = self.parse_adjacent_by()? {
                out.push(by);
            }
            if let Some((op, value)) = self.parse_numeric_filter()? {
                out.push(Pipeline::Filter { op, value });
            }
        }
        Ok(out)
    }

    pub(crate) fn parse_pipeline_stage(&mut self) -> Result<Pipeline> {
        let name = self.expect_ident()?;
        match name.as_str() {
            "count" => self.parse_empty_pipeline_stage(Pipeline::Aggregate(Aggregate::Count)),
            "rate" => self.parse_empty_pipeline_stage(Pipeline::Aggregate(Aggregate::Rate)),
            "count_over_time" => {
                self.parse_empty_pipeline_stage(Pipeline::Aggregate(Aggregate::CountOverTime))
            }
            "sum_over_time" | "avg_over_time" | "min_over_time" | "max_over_time" => {
                self.parse_field_over_time(&name)
            }
            "histogram_over_time" => self.parse_histogram_over_time(),
            "quantile_over_time" => {
                self.expect(&Token::LParen)?;
                let field = self.parse_field()?;
                let mut quantiles = Vec::new();
                while eat!(self, &Token::Comma) {
                    quantiles.push(self.parse_quantile()?);
                }
                self.expect(&Token::RParen)?;
                Ok(Pipeline::Aggregate(Aggregate::QuantileOverTime {
                    field,
                    quantiles,
                }))
            }
            "sum" | "avg" | "max" | "min" => self.parse_field_aggregate(&name),
            "by" => Ok(Pipeline::By(self.parse_parenthesized_field_list()?)),
            "topk" => {
                self.expect(&Token::LParen)?;
                let k = self.parse_rank_limit()?;
                self.expect(&Token::RParen)?;
                Ok(Pipeline::TopK(k))
            }
            "bottomk" => {
                self.expect(&Token::LParen)?;
                let k = self.parse_rank_limit()?;
                self.expect(&Token::RParen)?;
                Ok(Pipeline::BottomK(k))
            }
            "compare" => self.parse_compare(),
            "coalesce" => self.parse_empty_pipeline_stage(Pipeline::Coalesce),
            "select" => Ok(Pipeline::Select(self.parse_parenthesized_field_list()?)),
            "with" => self.parse_with_pipeline(),
            other => Err(Self::err(format!("unsupported pipeline stage {other:?}"))),
        }
    }

    pub(crate) fn parse_empty_pipeline_stage(&mut self, stage: Pipeline) -> Result<Pipeline> {
        self.expect(&Token::LParen)?;
        self.expect(&Token::RParen)?;
        Ok(stage)
    }

    pub(crate) fn parse_field_aggregate(&mut self, name: &str) -> Result<Pipeline> {
        self.expect(&Token::LParen)?;
        let field = self.parse_field()?;
        self.expect(&Token::RParen)?;
        let aggregate = match name {
            "sum" => Aggregate::Sum(field),
            "avg" => Aggregate::Avg(field),
            "max" => Aggregate::Max(field),
            "min" => Aggregate::Min(field),
            _ => unreachable!("matched aggregate is exhaustive"),
        };
        Ok(Pipeline::Aggregate(aggregate))
    }

    /// Parses Tempo's attribute-comparison metric:
    /// `compare({selection}, topN [, start_ns, end_ns])`.
    ///
    /// The selection is a full spanset: `{...}`, `And`, or `Or`. This method
    /// reuses `parse_spanset_or` to parse it. `topN` is an optional positive
    /// integer, and defaults to 10. `start_ns` and `end_ns` are optional signed
    /// nanosecond bounds. The query must give both bounds or neither one.
    /// Grafana sends none or both.
    pub(crate) fn parse_compare(&mut self) -> Result<Pipeline> {
        self.expect(&Token::LParen)?;
        let selection = self.parse_spanset_or()?;
        let top_n = if eat!(self, &Token::Comma) {
            self.parse_compare_top_n()?
        } else {
            DEFAULT_COMPARE_TOP_N
        };
        let (start, end) = if eat!(self, &Token::Comma) {
            let start = self.parse_signed_int()?;
            self.expect(&Token::Comma)?;
            let end = self.parse_signed_int()?;
            (Some(start), Some(end))
        } else {
            (None, None)
        };
        self.expect(&Token::RParen)?;
        Ok(Pipeline::Compare {
            selection: Box::new(selection),
            top_n,
            start,
            end,
        })
    }

    pub(crate) fn parse_compare_top_n(&mut self) -> Result<usize> {
        let value = self.parse_signed_int()?;
        if value < 0 {
            return Err(Self::err("compare topN must be non-negative"));
        }
        usize::try_from(value).map_err(|e| TraceqlError::Parse(e.to_string()))
    }

    /// Parses a signed integer literal, with an optional minus sign.
    ///
    /// The compare nanosecond bounds use this method. `-5` lexes as
    /// `Minus Int`, so this method consumes the leading `-` here. The parser
    /// does not fold the value later.
    pub(crate) fn parse_signed_int(&mut self) -> Result<i64> {
        let negative = eat!(self, &Token::Minus);
        let Token::Int(value) = self.advance() else {
            return Err(Self::err("expected integer literal"));
        };
        if negative {
            value
                .checked_neg()
                .ok_or_else(|| Self::err("integer negation out of range"))
        } else {
            Ok(value)
        }
    }

    pub(crate) fn parse_with_pipeline(&mut self) -> Result<Pipeline> {
        self.expect(&Token::LParen)?;
        let mut bindings = Vec::new();
        loop {
            let name = self.expect_ident()?;
            self.expect(&Token::Eq)?;
            let expr = self.parse_field_or()?;
            bindings.push(WithBinding { name, expr });
            if !eat!(self, &Token::Comma) {
                break;
            }
        }
        self.expect(&Token::RParen)?;
        Ok(Pipeline::With(bindings))
    }

    pub(crate) fn parse_field_over_time(&mut self, name: &str) -> Result<Pipeline> {
        self.expect(&Token::LParen)?;
        let field = self.parse_field()?;
        self.expect(&Token::RParen)?;
        let aggregate = match name {
            "sum_over_time" => Aggregate::SumOverTime(field),
            "avg_over_time" => Aggregate::AvgOverTime(field),
            "min_over_time" => Aggregate::MinOverTime(field),
            "max_over_time" => Aggregate::MaxOverTime(field),
            _ => unreachable!("matched aggregate is exhaustive"),
        };
        Ok(Pipeline::Aggregate(aggregate))
    }

    pub(crate) fn parse_histogram_over_time(&mut self) -> Result<Pipeline> {
        self.expect(&Token::LParen)?;
        let field = if self.peek() == &Token::RParen {
            Field {
                scope: Scope::Intrinsic(Intrinsic::Duration),
                key: "duration".into(),
            }
        } else {
            self.parse_field()?
        };
        self.expect(&Token::RParen)?;
        Ok(Pipeline::Aggregate(Aggregate::HistogramOverTime(field)))
    }

    pub(crate) fn parse_adjacent_by(&mut self) -> Result<Option<Pipeline>> {
        if !matches!(self.peek(), Token::Ident(name) if name == "by") {
            return Ok(None);
        }
        self.pos += 1;
        Ok(Some(Pipeline::By(self.parse_parenthesized_field_list()?)))
    }

    pub(crate) fn parse_parenthesized_field_list(&mut self) -> Result<Vec<Field>> {
        self.expect(&Token::LParen)?;
        let fields = self.parse_field_list()?;
        self.expect(&Token::RParen)?;
        Ok(fields)
    }

    pub(crate) fn parse_rank_limit(&mut self) -> Result<usize> {
        let Token::Int(value) = self.advance() else {
            return Err(Self::err("expected integer rank limit"));
        };
        if value < 0 {
            return Err(Self::err("rank limit must be non-negative"));
        }
        usize::try_from(value).map_err(|e| TraceqlError::Parse(e.to_string()))
    }

    pub(crate) fn parse_numeric_filter(&mut self) -> Result<Option<(ComparisonOp, f64)>> {
        let Some(op) = self.parse_comparison_op() else {
            return Ok(None);
        };
        let value = numeric_filter_value(self.parse_additive_value(&numeric_filter_field())?)?;
        Ok(Some((op, value)))
    }

    pub(crate) fn parse_field_list(&mut self) -> Result<Vec<Field>> {
        let mut fields = vec![self.parse_field()?];
        while eat!(self, &Token::Comma) {
            fields.push(self.parse_field()?);
        }
        Ok(fields)
    }

    pub(crate) fn parse_quantile(&mut self) -> Result<f64> {
        let value = if eat!(self, &Token::Dot) {
            let digits = match self.advance() {
                Token::Int(v) => v.to_string(),
                other => {
                    return Err(Self::err(format!(
                        "expected quantile digits, got {other:?}"
                    )));
                }
            };
            format!("0.{digits}")
                .parse()
                .map_err(|e: std::num::ParseFloatError| TraceqlError::Parse(e.to_string()))?
        } else {
            match self.advance() {
                Token::Float(v) => v,
                Token::Int(v) => v
                    .to_string()
                    .parse()
                    .map_err(|e: std::num::ParseFloatError| TraceqlError::Parse(e.to_string()))?,
                other => return Err(Self::err(format!("expected quantile, got {other:?}"))),
            }
        };
        if !(0.0..=1.0).contains(&value) {
            return Err(Self::err(format!("quantile out of range: {value}")));
        }
        Ok(value)
    }

    pub(crate) fn parse_spanset_or(&mut self) -> Result<SpansetExpr> {
        let mut expr = self.parse_spanset_and()?;
        while eat!(self, &Token::Or) {
            let rhs = self.parse_spanset_and()?;
            expr = SpansetExpr::Or(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    pub(crate) fn parse_spanset_and(&mut self) -> Result<SpansetExpr> {
        let mut expr = self.parse_structural()?;
        while eat!(self, &Token::And) {
            let rhs = self.parse_structural()?;
            expr = SpansetExpr::And(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    pub(crate) fn parse_structural(&mut self) -> Result<SpansetExpr> {
        let mut expr = self.parse_spanset_primary()?;
        while let Some(op) = self.parse_structural_op() {
            let rhs = self.parse_spanset_primary()?;
            expr = SpansetExpr::Structural {
                op,
                lhs: Box::new(expr),
                rhs: Box::new(rhs),
            };
        }
        Ok(expr)
    }

    pub(crate) fn parse_spanset_primary(&mut self) -> Result<SpansetExpr> {
        if eat!(self, &Token::LBrace) {
            // `{}` is the match-all spanset (every span). TraceQL treats empty
            // braces as a constant-true filter, so don't require a field
            // expression — Grafana's Tempo Explore sends `{}` by default.
            if eat!(self, &Token::RBrace) {
                return Ok(SpansetExpr::Selector(Box::new(FieldExpr::Const(true))));
            }
            let fe = self.parse_field_or()?;
            self.expect(&Token::RBrace)?;
            return Ok(SpansetExpr::Selector(Box::new(fe)));
        }
        if eat!(self, &Token::LParen) {
            let expr = self.parse_spanset_or()?;
            self.expect(&Token::RParen)?;
            return Ok(expr);
        }
        Err(Self::err(format!(
            "expected spanset, got {:?}",
            self.peek()
        )))
    }

    pub(crate) fn parse_field_or(&mut self) -> Result<FieldExpr> {
        let mut expr = self.parse_field_and()?;
        while eat!(self, &Token::Or) {
            let rhs = self.parse_field_and()?;
            expr = FieldExpr::Or(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    pub(crate) fn parse_field_and(&mut self) -> Result<FieldExpr> {
        let mut expr = self.parse_field_not()?;
        while eat!(self, &Token::And) {
            let rhs = self.parse_field_not()?;
            expr = FieldExpr::And(Box::new(expr), Box::new(rhs));
        }
        Ok(expr)
    }

    pub(crate) fn parse_field_not(&mut self) -> Result<FieldExpr> {
        if eat!(self, &Token::Not) {
            return Ok(FieldExpr::Not(Box::new(self.parse_field_not()?)));
        }
        self.parse_comparison()
    }

    pub(crate) fn parse_comparison(&mut self) -> Result<FieldExpr> {
        if eat!(self, &Token::LParen) {
            let expr = self.parse_field_or()?;
            self.expect(&Token::RParen)?;
            return Ok(expr);
        }
        // A bare boolean literal is a constant filter: `{ true }` matches every
        // span, `{ false }` none. Only a *leading* bool is a constant — a bool on
        // the right of a comparison (`{ .ok = true }`) is handled by parse_value.
        if let Token::Bool(value) = self.peek() {
            let value = *value;
            self.pos += 1;
            return Ok(FieldExpr::Const(value));
        }
        let lhs = self.parse_field()?;
        let Some(op) = self.parse_comparison_op() else {
            return Ok(FieldExpr::Field(lhs));
        };
        if op == ComparisonOp::Eq && self.peek() == &Token::Eq {
            return Err(Self::err("use single = for equality; == is not TraceQL"));
        }
        let rhs = self.parse_value(&lhs)?;
        Ok(FieldExpr::Comparison { lhs, op, rhs })
    }

    pub(crate) fn parse_field(&mut self) -> Result<Field> {
        if eat!(self, &Token::Dot) {
            return Ok(Field {
                scope: Scope::Both,
                key: self.expect_ident()?,
            });
        }

        let first = self.expect_ident()?;
        if eat!(self, &Token::Colon) {
            let key = self.expect_ident()?;
            return Ok(Field {
                scope: Scope::Intrinsic(intrinsic(&first, &key)?),
                key,
            });
        }
        if eat!(self, &Token::Dot) {
            return Ok(Field {
                scope: scope(&first)?,
                key: self.expect_ident()?,
            });
        }
        // A bare identifier matching a reserved intrinsic name is the intrinsic,
        // not an attribute. Tempo treats `duration`, `name`, `nestedSetParent`,
        // etc. as intrinsics when written scopeless; only `.foo` / `span.foo` /
        // `resource.foo` are attribute lookups.
        if let Some(intrinsic) = scopeless_intrinsic(&first) {
            return Ok(Field {
                scope: Scope::Intrinsic(intrinsic),
                key: first,
            });
        }
        Ok(Field {
            scope: Scope::Both,
            key: first,
        })
    }

    pub(crate) fn parse_value(&mut self, lhs: &Field) -> Result<Value> {
        self.parse_additive_value(lhs)
    }

    pub(crate) fn parse_additive_value(&mut self, lhs: &Field) -> Result<Value> {
        let mut value = self.parse_multiplicative_value(lhs)?;
        loop {
            if eat!(self, &Token::Plus) {
                value = value_add(value, self.parse_multiplicative_value(lhs)?)?;
            } else if eat!(self, &Token::Minus) {
                value = value_sub(value, self.parse_multiplicative_value(lhs)?)?;
            } else {
                return Ok(value);
            }
        }
    }

    pub(crate) fn parse_multiplicative_value(&mut self, lhs: &Field) -> Result<Value> {
        let mut value = self.parse_power_value(lhs)?;
        loop {
            if eat!(self, &Token::Star) {
                value = value_mul(value, self.parse_power_value(lhs)?)?;
            } else if eat!(self, &Token::Slash) {
                value = value_div(value, self.parse_power_value(lhs)?)?;
            } else if eat!(self, &Token::Mod) {
                value = value_mod(value, self.parse_power_value(lhs)?)?;
            } else {
                return Ok(value);
            }
        }
    }

    pub(crate) fn parse_power_value(&mut self, lhs: &Field) -> Result<Value> {
        let mut value = self.parse_unary_value(lhs)?;
        while eat!(self, &Token::Caret) {
            value = value_pow(value, self.parse_unary_value(lhs)?)?;
        }
        Ok(value)
    }

    pub(crate) fn parse_unary_value(&mut self, lhs: &Field) -> Result<Value> {
        if eat!(self, &Token::Minus) {
            return value_neg(self.parse_unary_value(lhs)?);
        }
        self.parse_primary_value(lhs)
    }

    pub(crate) fn parse_primary_value(&mut self, lhs: &Field) -> Result<Value> {
        match self.advance() {
            Token::Ident(v) if is_duration_field(lhs) => {
                parse_duration_nanos(&v).map(Value::Duration)
            }
            Token::Str(v) | Token::Ident(v) => Ok(Value::Str(v)),
            Token::Int(v) => Ok(Value::Int(v)),
            Token::Float(v) => Ok(Value::Float(v)),
            Token::Bool(v) => Ok(Value::Bool(v)),
            Token::Nil => Ok(Value::Nil),
            Token::LParen => {
                let value = self.parse_additive_value(lhs)?;
                self.expect(&Token::RParen)?;
                Ok(value)
            }
            other => Err(Self::err(format!("expected value, got {other:?}"))),
        }
    }

    pub(crate) fn parse_comparison_op(&mut self) -> Option<ComparisonOp> {
        let op = match self.peek() {
            Token::Eq => ComparisonOp::Eq,
            Token::Neq => ComparisonOp::Neq,
            Token::Parent => ComparisonOp::Lt,
            Token::Lte => ComparisonOp::Lte,
            Token::Child => ComparisonOp::Gt,
            Token::Gte => ComparisonOp::Gte,
            Token::Re => ComparisonOp::Re,
            Token::Nre => ComparisonOp::Nre,
            _ => return None,
        };
        self.pos += 1;
        Some(op)
    }

    pub(crate) fn parse_structural_op(&mut self) -> Option<StructuralOp> {
        let op = match self.peek() {
            Token::Desc => StructuralOp::Descendant,
            Token::Anc => StructuralOp::Ancestor,
            Token::Child => StructuralOp::Child,
            Token::Parent => StructuralOp::Parent,
            Token::Sibling => StructuralOp::Sibling,
            Token::NegDesc => StructuralOp::NegDescendant,
            Token::NegAnc => StructuralOp::NegAncestor,
            Token::NegChild => StructuralOp::NegChild,
            Token::NegParent => StructuralOp::NegParent,
            Token::UnionDesc => StructuralOp::UnionDescendant,
            Token::UnionAnc => StructuralOp::UnionAncestor,
            Token::UnionChild => StructuralOp::UnionChild,
            Token::UnionParent => StructuralOp::UnionParent,
            Token::UnionSibling => StructuralOp::UnionSibling,
            _ => return None,
        };
        self.pos += 1;
        Some(op)
    }

    pub(crate) fn expect_ident(&mut self) -> Result<String> {
        match self.advance() {
            Token::Ident(s) => Ok(s),
            other => Err(Self::err(format!("expected identifier, got {other:?}"))),
        }
    }

    pub(crate) fn expect(&mut self, expected: &Token) -> Result<()> {
        let got = self.advance();
        if &got == expected {
            Ok(())
        } else {
            Err(Self::err(format!("expected {expected:?}, got {got:?}")))
        }
    }

    pub(crate) fn eat(&mut self, expected: &Token) -> bool {
        if self.peek() == expected {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    pub(crate) fn advance(&mut self) -> Token {
        let t = self.peek().clone();
        self.pos += 1;
        t
    }

    pub(crate) fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    pub(crate) fn err(msg: impl Into<String>) -> TraceqlError {
        TraceqlError::Parse(msg.into())
    }
}
