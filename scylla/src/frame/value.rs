use bytes::{BufMut, BytesMut};
use std::convert::TryInto;
use thiserror::Error;

/// Defines a way to serialize a value type to [bytes]
pub trait SerializeAsValue {
    fn serialize(&self, buf: &mut BytesMut) -> Result<usize, ValueTooBig>;
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[error("Value too big to be sent in a request - max 2GiB allowed")]
pub struct ValueTooBig;

/// Keeps a list of serialized values and allows to iterate over them
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SerializedValues {
    serialized_values: BytesMut,
    values_num: i16,
}

impl SerializedValues {
    /// Creates empty value list - 0 allocations
    pub fn new() -> Self {
        SerializedValues {
            serialized_values: BytesMut::new(),
            values_num: 0,
        }
    }

    /// Serializes value and appends it to the list
    pub fn add_value(&mut self, val: &impl SerializeAsValue) -> Result<(), AddValueError> {
        if self.values_num == i16::max_value() {
            return Err(AddValueError::TooManyValues);
        }

        let len_before_serialize: usize = self.serialized_values.len();

        if let Err(e) = val.serialize(&mut self.serialized_values) {
            self.serialized_values.resize(len_before_serialize, 0);
            return Err(AddValueError::from(e));
        }

        self.values_num += 1;
        Ok(())
    }

    pub fn iter(&self) -> SerializedValuesIterator {
        SerializedValuesIterator {
            serialized_values: &self.serialized_values,
            next_offset: 0,
        }
    }

    pub fn write_to_request(&self, buf: &mut impl BufMut) {
        buf.put_i16(self.values_num);
        buf.put(&self.serialized_values[..]);
    }

    pub fn is_empty(&self) -> bool {
        self.values_num == 0
    }

    pub fn len(&self) -> i16 {
        self.values_num
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AddValueError {
    #[error("Too many values to add, max 32 767 values can be sent in a request")]
    TooManyValues,
    #[error(transparent)]
    ValueTooBig(#[from] ValueTooBig),
}

impl Default for SerializedValues {
    fn default() -> Self {
        SerializedValues::new()
    }
}

#[derive(Clone, Copy)]
pub struct SerializedValuesIterator<'a> {
    serialized_values: &'a [u8],
    next_offset: usize,
}

impl<'a> Iterator for SerializedValuesIterator<'a> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        // Read next value's 4 byte size, return it, advance

        if self.next_offset + 4 > self.serialized_values.len() {
            // Reached the end - nothing more to read
            return None;
        }

        let len_bytes: [u8; 4] = self.serialized_values[self.next_offset..(self.next_offset + 4)]
            .try_into()
            .unwrap();

        let next_val_len: i32 = i32::from_be_bytes(len_bytes);

        if next_val_len < 0 {
            // Next value was NULL
            self.next_offset += 4;
            return Some(None);
        }

        // Found next value - get the slice and return it
        let val_len: usize = next_val_len.try_into().unwrap();
        let result: &[u8] =
            &self.serialized_values[(self.next_offset + 4)..(self.next_offset + 4 + val_len)];
        self.next_offset += 4 + val_len;
        Some(Some(result))
    }
}

/// Every Option<T> can be serialized as None -> NULL, Some -> serialize()
impl<T: SerializeAsValue> SerializeAsValue for Option<T> {
    fn serialize(&self, buf: &mut BytesMut) -> Result<usize, ValueTooBig> {
        match self {
            Some(val) => val.serialize(buf),
            None => {
                buf.put_i32(-1);
                Ok(4)
            }
        }
    }
}

impl SerializeAsValue for &str {
    fn serialize(&self, buf: &mut BytesMut) -> Result<usize, ValueTooBig> {
        let str_bytes: &[u8] = self.as_bytes();
        let val_len: i32 = str_bytes.len().try_into().map_err(|_| ValueTooBig)?;

        buf.put_i32(val_len);
        buf.put(str_bytes);

        Ok(4 + str_bytes.len())
    }
}

impl SerializeAsValue for String {
    fn serialize(&self, buf: &mut BytesMut) -> Result<usize, ValueTooBig> {
        self.as_str().serialize(buf)
    }
}

impl SerializeAsValue for i8 {
    fn serialize(&self, buf: &mut BytesMut) -> Result<usize, ValueTooBig> {
        buf.put_i32(1);
        buf.put_i8(*self);
        Ok(4 + 1)
    }
}

impl SerializeAsValue for i16 {
    fn serialize(&self, buf: &mut BytesMut) -> Result<usize, ValueTooBig> {
        buf.put_i32(2);
        buf.put_i16(*self);
        Ok(4 + 2)
    }
}

impl SerializeAsValue for i32 {
    fn serialize(&self, buf: &mut BytesMut) -> Result<usize, ValueTooBig> {
        buf.put_i32(4);
        buf.put_i32(*self);
        Ok(4 + 4)
    }
}

impl SerializeAsValue for i64 {
    fn serialize(&self, buf: &mut BytesMut) -> Result<usize, ValueTooBig> {
        buf.put_i32(8);
        buf.put_i64(*self);
        Ok(4 + 8)
    }
}

impl SerializeAsValue for u8 {
    fn serialize(&self, buf: &mut BytesMut) -> Result<usize, ValueTooBig> {
        buf.put_u32(1);
        buf.put_u8(*self);
        Ok(4 + 1)
    }
}

impl SerializeAsValue for u16 {
    fn serialize(&self, buf: &mut BytesMut) -> Result<usize, ValueTooBig> {
        buf.put_u32(2);
        buf.put_u16(*self);
        Ok(4 + 2)
    }
}

impl SerializeAsValue for u32 {
    fn serialize(&self, buf: &mut BytesMut) -> Result<usize, ValueTooBig> {
        buf.put_u32(4);
        buf.put_u32(*self);
        Ok(4 + 4)
    }
}

impl SerializeAsValue for u64 {
    fn serialize(&self, buf: &mut BytesMut) -> Result<usize, ValueTooBig> {
        buf.put_u32(8);
        buf.put_u64(*self);
        Ok(4 + 8)
    }
}

#[cfg(test)]
mod tests {
    use super::SerializedValues;
    use crate::values;

    #[test]
    fn basic_numbers() {
        macro_rules! assert_ser {
            ($($value:expr),*; $expected_ref:expr) => {
                let expected: &[u8] = $expected_ref;
                let values: SerializedValues = values!($($value),*).unwrap();

                assert_eq!(values.serialized_values, expected);
            };
        }

        assert_ser!(1_i8; &[0, 0, 0, 1, 1]);
        assert_ser!(1_i16; &[0, 0, 0, 2, 0, 1]);
        assert_ser!(1_i32; &[0, 0, 0, 4, 0, 0, 0, 1]);
        assert_ser!(1_i64; &[0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1]);

        assert_ser!(1_u8; &[0, 0, 0, 1, 1]);
        assert_ser!(1_u16; &[0, 0, 0, 2, 0, 1]);
        assert_ser!(1_u32; &[0, 0, 0, 4, 0, 0, 0, 1]);
        assert_ser!(1_u64; &[0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1]);

        assert_ser!(1_i8, 2_i16, 3_i32; &[0, 0, 0, 1, 1, 0, 0, 0, 2, 0, 2, 0, 0, 0, 4, 0, 0, 0, 3]);
    }

    #[test]
    fn iterator() {
        let null_i32: Option<i32> = None;
        let vals: SerializedValues =
            values!(1_i8, 2_u64, null_i32, "1234", String::from("abcde")).unwrap();

        let mut iter = vals.iter();
        assert_eq!(iter.next(), Some(Some([1].as_ref())));
        assert_eq!(iter.next(), Some(Some([0, 0, 0, 0, 0, 0, 0, 2].as_ref())));
        assert_eq!(iter.next(), Some(None));
        assert_eq!(iter.next(), Some(Some(b"1234".as_ref())));
        assert_eq!(iter.next(), Some(Some(b"abcde".as_ref())));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }
}
