use super::*;

#[derive(Clone, Copy)]
pub(crate) enum SetOp {
    And,
    Or,
    Unless,
}

impl SetOp {
    pub(crate) fn from_token(token: TokenType) -> Option<Self> {
        match token.id() {
            T_LAND => Some(Self::And),
            T_LOR => Some(Self::Or),
            T_LUNLESS => Some(Self::Unless),
            _ => None,
        }
    }
}
