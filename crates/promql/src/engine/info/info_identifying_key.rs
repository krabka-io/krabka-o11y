use super::*;

pub(crate) fn info_identifying_key(labels: &Labels) -> Option<String> {
    Some(format!(
        "job={}\ninstance={}\n",
        labels.get("job")?,
        labels.get("instance")?
    ))
}
