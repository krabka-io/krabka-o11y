use super::*;

#[derive(Clone, Debug, PartialEq)]
pub enum PipelineStage {
    LineFilter(LineFilter),
    Decolorize,
    Parser(ParserStage),
    LineFormat(LineFormat),
    LabelFormat(LabelFormat),
    DropLabels(LabelSelectionSet),
    KeepLabels(LabelSelectionSet),
    Unwrap(UnwrapExpression),
    FieldFilter(FieldFilter),
    FieldFilterChain(FieldFilterChain),
    FieldFilterExpression(FieldFilterExpression),
}

impl PipelineStage {
    #[must_use]
    pub fn matches(&self, line: &str) -> bool {
        self.apply(&mut line.to_string(), &mut Labels::new())
    }

    #[must_use]
    pub fn apply(&self, line: &mut String, fields: &mut Labels) -> bool {
        self.apply_with_timestamp(line, fields, None)
    }

    pub(crate) fn apply_with_timestamp(
        &self,
        line: &mut String,
        fields: &mut Labels,
        timestamp_ns: Option<i64>,
    ) -> bool {
        match self {
            Self::LineFilter(filter) => filter.matches(line),
            Self::Decolorize => {
                *line = decolorize_line(line);
                true
            }
            Self::Parser(parser) => {
                parser.apply(line, fields);
                true
            }
            Self::LineFormat(format) => {
                *line = format.render_with_timestamp(line, fields, timestamp_ns);
                true
            }
            Self::LabelFormat(format) => {
                format.apply_with_timestamp(line, fields, timestamp_ns);
                true
            }
            Self::DropLabels(labels) => {
                labels.apply_drop(fields);
                true
            }
            Self::KeepLabels(labels) => {
                labels.apply_keep(fields);
                true
            }
            Self::Unwrap(unwrap) => {
                unwrap.apply(fields);
                true
            }
            Self::FieldFilter(filter) => filter.apply(fields),
            Self::FieldFilterChain(chain) => chain.apply(fields),
            Self::FieldFilterExpression(expression) => expression.apply(fields),
        }
    }

    #[must_use]
    pub fn mutates_line(&self) -> bool {
        matches!(
            self,
            Self::Decolorize | Self::Parser(ParserStage::Unpack) | Self::LineFormat(_)
        )
    }
}
