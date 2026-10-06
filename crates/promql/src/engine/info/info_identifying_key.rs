use super::Labels;

pub(crate) fn info_identifying_key(labels: &Labels) -> Option<String> {
    if labels.get("job").is_none() && labels.get("instance").is_none() {
        return None;
    }
    Some(format!(
        "job={}\ninstance={}\n",
        labels.get("job").unwrap_or(""),
        labels.get("instance").unwrap_or("")
    ))
}
