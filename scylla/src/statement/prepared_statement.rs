use bytes::Bytes;

#[derive(Debug)]
pub struct PreparedStatement {
    id: Bytes,
}

impl PreparedStatement {
    pub fn new(id: Bytes) -> Self {
        Self { id }
    }

    pub fn get_id(&self) -> &Bytes {
        &self.id
    }
}
