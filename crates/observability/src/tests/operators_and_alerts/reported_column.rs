/// The column a `LogQL` parse error message reports, from its `col N:` part.
pub(crate) fn reported_column(message: &str) -> usize {
    message
        .split("col ")
        .nth(1)
        .and_then(|rest| rest.split(':').next())
        .expect("the message names a column")
        .parse::<usize>()
        .expect("the column is a number")
}
