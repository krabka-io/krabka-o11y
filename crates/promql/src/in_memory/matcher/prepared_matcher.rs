use super::*;

pub(crate) enum PreparedMatcher {
    LabelEq { name: String, value: String },
    LabelNeq { name: String, value: String },
    LabelRe { name: String, regex: regex::Regex },
    LabelNre { name: String, regex: regex::Regex },
    QueryShardEq(QueryShardSelector),
    QueryShardNeq(QueryShardSelector),
}

impl PreparedMatcher {
    pub(crate) fn new(matcher: &LabelMatcher) -> Result<Self> {
        if matcher.name == QUERY_SHARD_LABEL {
            let selector = parse_query_shard_selector(&matcher.value).map_err(|error| {
                PromqlError::Plan(format!("invalid query shard matcher: {error}"))
            })?;
            return match matcher.op {
                MatchOp::Eq => Ok(Self::QueryShardEq(selector)),
                MatchOp::Neq => Ok(Self::QueryShardNeq(selector)),
                MatchOp::Re | MatchOp::Nre => Err(PromqlError::Plan(
                    "query shard matcher must use equality or inequality".into(),
                )),
            };
        }

        match matcher.op {
            MatchOp::Eq => Ok(Self::LabelEq {
                name: matcher.name.clone(),
                value: matcher.value.clone(),
            }),
            MatchOp::Neq => Ok(Self::LabelNeq {
                name: matcher.name.clone(),
                value: matcher.value.clone(),
            }),
            MatchOp::Re => Ok(Self::LabelRe {
                name: matcher.name.clone(),
                regex: regex_anchored(&matcher.value)?,
            }),
            MatchOp::Nre => Ok(Self::LabelNre {
                name: matcher.name.clone(),
                regex: regex_anchored(&matcher.value)?,
            }),
        }
    }

    pub(crate) fn matches(&self, fp: SeriesFingerprint, labels: &Labels) -> bool {
        match self {
            Self::LabelEq { name, value } => labels.get(name).unwrap_or("") == value.as_str(),
            Self::LabelNeq { name, value } => labels.get(name).unwrap_or("") != value.as_str(),
            Self::LabelRe { name, regex } => regex.is_match(labels.get(name).unwrap_or("")),
            Self::LabelNre { name, regex } => !regex.is_match(labels.get(name).unwrap_or("")),
            Self::QueryShardEq(selector) => selector.matches(fp),
            Self::QueryShardNeq(selector) => !selector.matches(fp),
        }
    }
}
