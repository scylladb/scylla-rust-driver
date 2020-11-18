use bytes::{BufMut, Bytes, BytesMut};
use std::convert::TryInto;

#[derive(Debug, Clone)]
pub enum Value {
    Val(Bytes),
    Null,
    NotSet,
}

pub trait TryIntoValue {
    fn try_into_value(self) -> Result<Value, ValueTooBig>;
}

pub struct ValueTooBig;

impl std::fmt::Debug for ValueTooBig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Value is too big to be sent in a request, max 2GiB allowed"
        )
    }
}

impl std::fmt::Display for ValueTooBig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Value is too big to be sent in a request, max 2GiB allowed"
        )
    }
}

impl std::error::Error for ValueTooBig {}

impl TryInto<Bytes> for Value {
    type Error = ValueTooBig;

    fn try_into(self) -> Result<Bytes, ValueTooBig> {
        const MIN_1: &[u8] = &(-1_i32).to_be_bytes();
        const MIN_2: &[u8] = &(-2_i32).to_be_bytes();

        match self {
            Value::Val(bytes) => {
                let bytes_len_int: i32 = match bytes.len().try_into() {
                    Ok(int_len) => int_len,
                    Err(_) => return Err(ValueTooBig),
                };

                let mut result = BytesMut::with_capacity(4 + bytes.len());

                result.put_i32(bytes_len_int);
                result.put(bytes);
                Ok(result.into())
            }
            Value::Null => Ok(Bytes::from_static(MIN_1)),
            Value::NotSet => Ok(Bytes::from_static(MIN_2)),
        }
    }
}

impl<T: TryIntoValue> TryIntoValue for Option<T> {
    fn try_into_value(self) -> Result<Value, ValueTooBig> {
        match self {
            Some(val) => Ok(<T as TryIntoValue>::try_into_value(val)?),
            None => Ok(Value::Null),
        }
    }
}

impl<T: Into<Value>> Into<Value> for Option<T> {
    fn into(self) -> Value {
        match self {
            Some(val) => <T as Into<Value>>::into(val),
            None => Value::Null,
        }
    }
}

// Given a type that implements Into<Value> implement TryIntoValue for it
// trying to do that with generics fails because TryIntoValue<Option<_>> is ambiguous
macro_rules! impl_try_into_for_into {
    ($t:ty) => {
        impl TryIntoValue for $t {
            fn try_into_value(self) -> Result<Value, ValueTooBig> {
                Ok(self.into())
            }
        }
    };
}

impl Into<Value> for String {
    fn into(self) -> Value {
        Value::Val(self.into())
    }
}

impl_try_into_for_into!(String);

impl Into<Value> for &str {
    fn into(self) -> Value {
        Value::Val(self.to_owned().into())
    }
}

impl_try_into_for_into!(&str);

impl Into<Value> for i8 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl_try_into_for_into!(i8);

impl Into<Value> for i16 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl_try_into_for_into!(i16);

impl Into<Value> for i32 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl_try_into_for_into!(i32);

impl Into<Value> for i64 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl_try_into_for_into!(i64);

impl Into<Value> for u8 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl_try_into_for_into!(u8);

impl Into<Value> for u16 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl_try_into_for_into!(u16);

impl Into<Value> for u32 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl_try_into_for_into!(u32);

impl Into<Value> for u64 {
    fn into(self) -> Value {
        Value::Val(self.to_be_bytes().to_vec().into())
    }
}

impl_try_into_for_into!(u64);
