use super::*;

pub(crate) fn epoch_template_timestamp(args: &[String], divisor: i64) -> String {
    let Some(timestamp) = args.first() else {
        return String::new();
    };
    let Ok(timestamp_ns) = timestamp.parse::<i64>() else {
        return String::new();
    };
    timestamp_ns.div_euclid(divisor).to_string()
}
