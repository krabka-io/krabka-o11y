use super::{PromqlError, Result};

pub(crate) fn top_level_colon(content: &str) -> Result<Option<usize>> {
    let chars = content.chars().collect::<Vec<_>>();
    let mut parens = 0_i32;
    let mut quote = None;
    for (index, ch) in chars.iter().enumerate() {
        if let Some(quote_ch) = quote {
            if *ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match *ch {
            '"' | '\'' | '`' => quote = Some(*ch),
            '(' => parens += 1,
            ')' => parens -= 1,
            ':' if parens == 0 => return Ok(Some(index)),
            _ => {}
        }
        if parens < 0 {
            return Err(PromqlError::Parse(format!(
                "unbalanced duration expression `{content}`"
            )));
        }
    }
    Ok(None)
}
