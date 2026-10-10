//! Prometheus v3.14.0 template/template.go source binding, SHA256
//! 68eb1a56702a4ac7d97ff3828257b34b222af73780d508870fc7dec7a443d2d9.
//! common v0.70.1 helpers/templates/time.go SHA256
//! 20e1f1ca79be0c4baae3027d6b600d141cf257d0363973c76ace58fd16ad1771.
use std::{collections::BTreeMap, net::IpAddr, sync::Arc};

use num_traits::ToPrimitive;

use super::{TemplateRenderContext, TemplateRuntimeValue as V};
use crate::util::hex_digit_value;
pub(super) mod histogram;
pub(super) mod histogram_value;
mod numeric;
pub(super) mod query_result;
mod title;

pub(super) fn is_name(name: &str) -> bool {
    matches!(
        name,
        "query"
            | "first"
            | "label"
            | "value"
            | "strvalue"
            | "args"
            | "reReplaceAll"
            | "safeHtml"
            | "match"
            | "title"
            | "toUpper"
            | "toLower"
            | "graphLink"
            | "tableLink"
            | "sortByLabel"
            | "stripPort"
            | "stripDomain"
            | "humanize"
            | "humanize1024"
            | "humanizeDuration"
            | "humanizePercentage"
            | "humanizeTimestamp"
            | "toTime"
            | "toDuration"
            | "now"
            | "pathPrefix"
            | "externalURL"
            | "parseDuration"
            | "urlQueryEscape"
    )
}
pub(super) fn is_builtin(name: &str) -> bool {
    matches!(
        name,
        "and"
            | "or"
            | "not"
            | "call"
            | "html"
            | "index"
            | "slice"
            | "js"
            | "len"
            | "print"
            | "printf"
            | "println"
            | "urlquery"
            | "eq"
            | "ne"
            | "lt"
            | "le"
            | "gt"
            | "ge"
    )
}
pub(super) fn is_loki_name(name: &str) -> bool {
    super::is_template_function_name(name) && (!is_name(name) || matches!(name, "now" | "title"))
}

pub(super) fn evaluate(
    name: &str,
    args: &[V],
    context: &TemplateRenderContext<'_>,
) -> Option<Result<V, String>> {
    if !is_name(name) || name == "query" {
        return None;
    }
    Some(run(name, args, context))
}
fn run(name: &str, args: &[V], context: &TemplateRenderContext<'_>) -> Result<V, String> {
    let count = match name {
        "args" => None,
        "now" | "pathPrefix" | "externalURL" => Some(0),
        "label" | "match" | "sortByLabel" => Some(2),
        "reReplaceAll" => Some(3),
        _ => Some(1),
    };
    if count.is_some_and(|count| args.len() != count) {
        return Err(format!("wrong number of args for {name}"));
    }
    if let Some(result) = numeric::evaluate(name, args) {
        return result;
    }
    let string = |index: usize| {
        args.get(index)
            .filter(|value| value.is_template_string())
            .and_then(V::string_bytes)
            .ok_or_else(|| format!("{name} requires a string argument"))
    };
    let result = match name {
        "now" => V::Float(
            context
                .prometheus_timestamp_ms
                .expect("Prometheus mode")
                .to_f64()
                .expect("i64 fits finite f64")
                / 1000.0,
        ),
        "first" => samples(&args[0])?
            .first()
            .cloned()
            .ok_or("first() called on vector with no elements")?,
        "label" => V::Bytes(sample_label(&args[1], string(0)?)?),
        "strvalue" => V::Bytes(sample_label(&args[0], b"__value__")?),
        "value" => sample(&args[0])?
            .get("Value")
            .or_else(|| sample(&args[0]).ok()?.get("value"))
            .cloned()
            .unwrap_or(V::Json(serde_json::Value::Null)),
        "args" => V::Object(
            args.iter()
                .enumerate()
                .map(|(index, value)| (format!("arg{index}"), value.clone()))
                .collect::<BTreeMap<_, _>>(),
        ),
        "safeHtml" => V::SafeHtml(string(0)?.to_vec()),
        "toUpper" | "toLower" => super::evaluate_template_byte_function(
            if name == "toUpper" { "upper" } else { "lower" },
            args,
        )
        .expect("case function"),
        "title" => V::Bytes(title::format(string(0)?)),
        "reReplaceAll" => {
            validate_regex(string(0)?)?;
            string(1)?;
            string(2)?;
            super::evaluate_template_byte_function(name, args)
                .ok_or("invalid regular expression")?
        }
        "match" => {
            let regex = validate_regex(string(0)?)?;
            let value = super::template_bytes_to_string(string(1)?);
            V::Json(serde_json::Value::Bool(regex.is_match(&value)))
        }
        "urlQueryEscape" => {
            super::evaluate_template_byte_function("urlencode", args).expect("URL escaping")
        }
        "graphLink" | "tableLink" => {
            string(0)?;
            let escaped = super::evaluate_template_byte_function("urlencode", args)
                .expect("URL escaping")
                .as_rendered_string();
            V::String(format!(
                "/graph?g0.expr={escaped}&g0.tab={}",
                u8::from(name == "tableLink")
            ))
        }
        "sortByLabel" => {
            let label = string(0)?;
            let V::QueryResult(values) = &args[1] else {
                return Err("expected template.queryResult".into());
            };
            values.sort_by_label(label)?;
            V::QueryResult(values.clone())
        }

        "stripPort" => V::Bytes(split_host_port(string(0)?).map_or_else(
            || string(0).expect("validated string").to_vec(),
            |(host, _)| host.to_vec(),
        )),
        "stripDomain" => V::Bytes(strip_domain(string(0)?)),
        "externalURL" | "pathPrefix" => {
            let external = external_url(context);
            V::Bytes(if name == "externalURL" {
                external
            } else {
                url_path(&external)
            })
        }
        _ => return Err(format!("unsupported Prometheus function {name}")),
    };
    Ok(result)
}
fn validate_regex(pattern: &[u8]) -> Result<regex::Regex, String> {
    let pattern = std::str::from_utf8(pattern).map_err(|_| "invalid UTF-8 in regexp")?;
    regex::Regex::new(pattern).map_err(|error| error.to_string())
}
fn sample(value: &V) -> Result<&BTreeMap<String, V>, String> {
    match value {
        V::Sample(value) => Ok(value),
        V::Object(value) => Ok(value),
        _ => Err("expected *template.sample".into()),
    }
}
fn samples(value: &V) -> Result<Vec<V>, String> {
    let values = match value {
        V::QueryResult(values) => values.snapshot(),
        V::Array(values) => values.clone(),
        _ => return Err("expected template.queryResult".into()),
    };
    values
        .into_iter()
        .map(|value| {
            sample(&value)?;
            Ok(match value {
                V::Sample(_) => value,
                V::Object(value) => V::Sample(Arc::new(value)),
                _ => unreachable!(),
            })
        })
        .collect()
}

fn sample_label(value: &V, key: &[u8]) -> Result<Vec<u8>, String> {
    let labels = sample(value)?
        .get("Labels")
        .or_else(|| sample(value).ok()?.get("labels"));
    let Some(labels) = labels else {
        return Ok(Vec::new());
    };
    let key = std::str::from_utf8(key).map_err(|_| "invalid label name")?;
    Ok(super::template_index_value(labels, key)
        .map_or_else(Vec::new, |value| value.rendered_bytes()))
}
fn split_host_port(value: &[u8]) -> Option<(&[u8], &[u8])> {
    if value.first() == Some(&b'[') {
        let close = value.iter().position(|byte| *byte == b']')?;
        if value.get(close + 1) != Some(&b':') || value[close + 2..].contains(&b':') {
            return None;
        }
        return Some((&value[1..close], &value[close + 2..]));
    }
    let colon = value.iter().position(|byte| *byte == b':')?;
    if value[colon + 1..].contains(&b':') || value.contains(&b'[') || value.contains(&b']') {
        return None;
    }
    Some((&value[..colon], &value[colon + 1..]))
}
fn strip_domain(value: &[u8]) -> Vec<u8> {
    let (host, port) = split_host_port(value).unwrap_or((value, b""));
    if std::str::from_utf8(host)
        .ok()
        .and_then(|host| host.parse::<IpAddr>().ok())
        .is_some()
    {
        return value.to_vec();
    }
    let host = host.split(|byte| *byte == b'.').next().unwrap_or_default();
    if port.is_empty() {
        host.to_vec()
    } else {
        [host, b":", port].concat()
    }
}
fn external_url(context: &TemplateRenderContext<'_>) -> Vec<u8> {
    context.external_url.as_bytes().to_vec()
}

fn url_path(value: &[u8]) -> Vec<u8> {
    let value = value
        .split(|byte| matches!(*byte, b'?' | b'#'))
        .next()
        .unwrap_or_default();
    let value = if let Some(scheme) = value.windows(3).position(|part| part == b"://") {
        let rest = &value[scheme + 3..];
        rest.iter()
            .position(|byte| *byte == b'/')
            .map_or(b"".as_slice(), |index| &rest[index..])
    } else {
        value
    };
    let mut output = Vec::new();
    let mut index = 0;
    while index < value.len() {
        if value[index] == b'%'
            && index + 2 < value.len()
            && let (Some(high), Some(low)) = (
                hex_digit_value(value[index + 1]),
                hex_digit_value(value[index + 2]),
            )
        {
            output.push(high * 16 + low);
            index += 3;
            continue;
        }
        output.push(value[index]);
        index += 1;
    }
    output
}

pub(super) fn query_result(value: V) -> V {
    match value {
        V::Array(values) => V::QueryResult(
            values
                .into_iter()
                .map(|value| match value {
                    V::Object(value) => V::Sample(Arc::new(value)),
                    value => value,
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        value => value,
    }
}
