use bytes::Bytes;

#[derive(Debug)]
pub enum Value {
    Val(Bytes),
    Null,
    NotSet,
}

impl Into<Value> for String {
    fn into(self) -> Value {
        Value::Val(self.into())
    }
}

impl<'a> Into<Value> for &'a str {
    fn into(self) -> Value {
        Value::Val(self.to_owned().into())
    }
}

impl Into<Value> for i8 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl Into<Value> for i16 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl Into<Value> for i32 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl Into<Value> for i64 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl Into<Value> for u8 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl Into<Value> for u16 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl Into<Value> for u32 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl Into<Value> for u64 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}
