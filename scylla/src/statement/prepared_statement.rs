use bytes::Bytes;

#[derive(Debug)]
pub struct PreparedStatement {
    id: Bytes,
    statement: String,
}

impl PreparedStatement {
    pub fn new(id: Bytes, statement: String) -> Self {
        Self { id, statement }
    }

    pub fn get_id(&self) -> &Bytes {
        &self.id
    }

    pub fn get_statement(&self) -> &str {
        &self.statement
    }
}
