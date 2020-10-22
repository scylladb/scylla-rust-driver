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

    pub fn update_id(&mut self, id: Bytes) {
        self.id = id
    }

    pub fn get_statement(&self) -> &str {
        &self.statement
    }
}
