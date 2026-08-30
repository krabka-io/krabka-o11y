use super::{
    BlockSchema, COL_FINGERPRINT, COL_TIMESTAMP, DataType, PCOL_PROFILE_TYPE, RequiredColumn,
    profile_type_dict,
};

#[must_use]
pub fn profile_samples_decl() -> BlockSchema {
    BlockSchema {
        required: vec![
            RequiredColumn::new(COL_FINGERPRINT, DataType::UInt64, false),
            RequiredColumn::new(PCOL_PROFILE_TYPE, profile_type_dict(), false),
            RequiredColumn::new(COL_TIMESTAMP, DataType::Int64, false),
        ],
        sort_key: vec![
            COL_FINGERPRINT.to_string(),
            PCOL_PROFILE_TYPE.to_string(),
            COL_TIMESTAMP.to_string(),
        ],
    }
}
