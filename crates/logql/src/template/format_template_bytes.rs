use num_traits::ToPrimitive;

use super::format_template_float;

pub(crate) fn format_template_bytes(value: &str) -> String {
    parse_template_bytes(value).map_or_else(String::new, format_template_float)
}

// Templates use go-humanize.ParseBytes; the LogQL filter grammar has a
// different, case-sensitive unit allowlist.
pub(crate) fn parse_template_bytes(value: &str) -> Option<f64> {
    let split = value
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.' || ch == ','))
        .unwrap_or(value.len());
    let number = value[..split].replace(',', "").parse::<f64>().ok()?;
    let unit = value[split..].trim().to_ascii_lowercase();
    let multiplier = match unit.as_str() {
        "" | "b" => 1.0,
        "k" | "kb" => 1_000.0,
        "m" | "mb" => 1_000_000.0,
        "g" | "gb" => 1_000_000_000.0,
        "t" | "tb" => 1_000_000_000_000.0,
        "p" | "pb" => 1_000_000_000_000_000.0,
        "e" | "eb" => 1_000_000_000_000_000_000.0,
        "ki" | "kib" => 1024.0,
        "mi" | "mib" => 1_048_576.0,
        "gi" | "gib" => 1_073_741_824.0,
        "ti" | "tib" => 1_099_511_627_776.0,
        "pi" | "pib" => 1_125_899_906_842_624.0,
        "ei" | "eib" => 1_152_921_504_606_846_976.0,
        _ => return None,
    };
    let bytes = number * multiplier;
    if !bytes.is_finite() || bytes >= 18_446_744_073_709_551_616.0 {
        return None;
    }
    bytes.to_u64()?.to_f64()
}
