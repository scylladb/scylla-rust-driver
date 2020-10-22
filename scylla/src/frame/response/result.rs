use anyhow::Result as AResult;

pub struct Result {
    // TODO: Implement ALL fields
}

impl Result {
    pub fn deserialize(_buf: &mut &[u8]) -> AResult<Self> {
        Ok(Result {})
    }
}
