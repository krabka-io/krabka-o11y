use super::PprofProfile;

impl From<PprofProfile> for crate::proto::Profile {
    fn from(profile: PprofProfile) -> Self {
        profile.inner
    }
}
