use super::MetricValue;

pub(crate) fn format_metric_value(value: MetricValue) -> String {
    let negative = value.numerator < 0;
    let numerator = value.numerator.unsigned_abs();
    let whole = numerator / value.denominator;
    let mut remainder = numerator % value.denominator;
    let sign = if negative { "-" } else { "" };
    if remainder == 0 {
        return format!("{sign}{whole}");
    }

    let mut decimals = String::new();
    while remainder != 0 && decimals.len() < 9 {
        remainder *= 10;
        let digit =
            u8::try_from(remainder / value.denominator).expect("decimal digit is less than 10");
        decimals.push(char::from(b'0' + digit));
        remainder %= value.denominator;
    }
    while decimals.ends_with('0') {
        decimals.pop();
    }
    format!("{sign}{whole}.{decimals}")
}
