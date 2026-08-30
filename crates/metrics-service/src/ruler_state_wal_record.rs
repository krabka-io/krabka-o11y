use super::*;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RulerStateWalRecord {
    Group(RulerGroupStateRecord),
    Alert(RulerAlertStateRecord),
}

impl RulerStateWalRecord {
    ///
    /// # Errors
    /// Returns an error if the operation cannot be completed.
    pub fn encode(&self) -> Result<Vec<u8>, RulerStateWalRecordError> {
        serde_json::to_vec(self)
            .map_err(|error| RulerStateWalRecordError::Encode(error.to_string()))
    }

    ///
    /// # Errors
    /// Returns an error if the operation cannot be completed.
    pub fn decode(bytes: &[u8]) -> Result<Self, RulerStateWalRecordError> {
        serde_json::from_slice(bytes)
            .map_err(|error| RulerStateWalRecordError::Decode(error.to_string()))
    }
}
