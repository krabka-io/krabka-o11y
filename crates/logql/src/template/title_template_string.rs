pub(crate) fn title_template_string(value: &str) -> String {
    let mut titled = String::new();
    let mut capitalize_next = true;
    for ch in value.chars() {
        if ch.is_alphanumeric() {
            if capitalize_next {
                for upper in ch.to_uppercase() {
                    titled.push(upper);
                }
            } else {
                for lower in ch.to_lowercase() {
                    titled.push(lower);
                }
            }
            capitalize_next = false;
        } else {
            titled.push(ch);
            capitalize_next = true;
        }
    }
    titled
}
