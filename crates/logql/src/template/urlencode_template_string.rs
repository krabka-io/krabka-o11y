use super::urlquery_template_string;

pub(crate) fn urlencode_template_string(value: &str) -> String {
    urlquery_template_string(value)
}
