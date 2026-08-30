pub(crate) fn urldecode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(byte) = bytes.next() {
        match byte {
            b'+' => out.push(' '),
            b'%' => {
                let (first, second) = (bytes.next(), bytes.next());
                let (Some(hi), Some(lo)) = (first, second) else {
                    // An escape cut short by the end of the input keeps
                    // whatever it consumed, the same way an unparseable one
                    // below does.
                    out.push('%');
                    if let Some(first) = first {
                        out.push(char::from(first));
                    }
                    continue;
                };
                let hex = [hi, lo];
                if let Ok(hex) = std::str::from_utf8(&hex)
                    && let Ok(decoded) = u8::from_str_radix(hex, 16)
                {
                    out.push(char::from(decoded));
                    continue;
                }
                out.push('%');
                out.push(char::from(hi));
                out.push(char::from(lo));
            }
            _ => out.push(char::from(byte)),
        }
    }
    out
}
