pub(crate) fn format_decimal_ratio(numerator: u128, denominator: u128) -> String {
    let whole = numerator / denominator;
    let mut remainder = numerator % denominator;
    if remainder == 0 {
        return whole.to_string();
    }

    let mut decimals = String::new();
    for _ in 0..9 {
        if remainder == 0 {
            break;
        }
        remainder *= 10;
        let digit = u8::try_from(remainder / denominator).expect("decimal digit is less than 10");
        decimals.push(char::from(b'0' + digit));
        remainder %= denominator;
    }
    while decimals.ends_with('0') {
        decimals.pop();
    }
    format!("{whole}.{decimals}")
}
