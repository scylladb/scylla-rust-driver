use bytes::BufMut;
use std::borrow::Cow;
use std::convert::TryInto;
use thiserror::Error;

/// Every value being sent in a query must implement this trait
/// serialize() should write the Value as [bytes] to the provided buffer
pub trait Value {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig>;
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[error("Value too big to be sent in a request - max 2GiB allowed")]
pub struct ValueTooBig;

/// Keeps a buffer with serialized Values
/// Allows adding new Values and iterating over serialized ones
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SerializedValues {
    serialized_values: Vec<u8>,
    values_num: i16,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SerializeValuesError {
    #[error("Too many values to add, max 32 767 values can be sent in a request")]
    TooManyValues,
    #[error(transparent)]
    ValueTooBig(#[from] ValueTooBig),
}

pub type SerializedResult<'a> = Result<Cow<'a, SerializedValues>, SerializeValuesError>;

/// Represents list of values to be sent in a query
/// gets serialized and but into request
pub trait ValueList {
    /// Provides a view of ValueList as SerializedValues
    /// returns Cow<SerializedValues> to make impl ValueList for SerializedValues efficient
    fn serialized(&self) -> SerializedResult<'_>;

    fn write_to_request(&self, buf: &mut impl BufMut) -> Result<(), SerializeValuesError> {
        let serialized = self.serialized()?;
        SerializedValues::write_to_request(&serialized, buf);

        Ok(())
    }
}

/// Represents List of ValueList for Batch statement
pub trait BatchValues {
    fn len(&self) -> usize;

    fn write_nth_to_request(
        &self,
        n: usize,
        buf: &mut impl BufMut,
    ) -> Result<(), SerializeValuesError>;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl SerializedValues {
    /// Creates empty value list
    pub const fn new() -> Self {
        SerializedValues {
            serialized_values: Vec::new(),
            values_num: 0,
        }
    }

    /// Serializes value and appends it to the list
    pub fn add_value(&mut self, val: &impl Value) -> Result<(), SerializeValuesError> {
        if self.values_num == i16::max_value() {
            return Err(SerializeValuesError::TooManyValues);
        }

        let len_before_serialize: usize = self.serialized_values.len();

        if let Err(e) = val.serialize(&mut self.serialized_values) {
            self.serialized_values.resize(len_before_serialize, 0);
            return Err(SerializeValuesError::from(e));
        }

        self.values_num += 1;
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&[u8]>> {
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

//
//  Value impls
//

// Implement Value for primitive types
impl Value for i8 {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(1);
        buf.put_i8(*self);
        Ok(())
    }
}

impl Value for i16 {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(2);
        buf.put_i16(*self);
        Ok(())
    }
}

impl Value for i32 {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(4);
        buf.put_i32(*self);
        Ok(())
    }
}

impl Value for i64 {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        buf.put_i32(8);
        buf.put_i64(*self);
        Ok(())
    }
}

impl Value for &str {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        let str_bytes: &[u8] = self.as_bytes();
        let val_len: i32 = str_bytes.len().try_into().map_err(|_| ValueTooBig)?;

        buf.put_i32(val_len);
        buf.put(str_bytes);

        Ok(())
    }
}

impl Value for String {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        <&str as Value>::serialize(&self.as_str(), buf)
    }
}

/// Every Option<T> can be serialized as None -> NULL, Some(val) -> val.serialize()
impl<T: Value> Value for Option<T> {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        match self {
            Some(val) => <T as Value>::serialize(val, buf),
            None => {
                buf.put_i32(-1);
                Ok(())
            }
        }
    }
}

// Every &impl Value should also implement Value
impl<T: Value> Value for &T {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        <T as Value>::serialize(*self, buf)
    }
}

//
//  ValueList impls
//

// Implement ValueList for the unit type
impl ValueList for () {
    fn serialized(&self) -> SerializedResult<'_> {
        Ok(Cow::Owned(SerializedValues::new()))
    }
}

impl ValueList for [u8; 0] {
    fn serialized(&self) -> SerializedResult<'_> {
        Ok(Cow::Owned(SerializedValues::new()))
    }
}

// Implement ValueList for slices of Value types
impl<T: Value> ValueList for &[T] {
    fn serialized(&self) -> SerializedResult<'_> {
        let mut result = SerializedValues::new();
        for val in *self {
            result.add_value(val)?;
        }

        Ok(Cow::Owned(result))
    }
}

// Implement ValueList for tuples of Values of size up to 16

// Here is an example implemetation for (T0, )
// Further variants are done using a macro
impl<T0: Value> ValueList for (T0,) {
    fn serialized(&self) -> SerializedResult<'_> {
        let mut result = SerializedValues::new();
        result.add_value(&self.0)?;
        Ok(Cow::Owned(result))
    }
}

macro_rules! impl_value_list_for_tuple {
    ( $($Ti:ident),* ; $($FieldI:tt),* ) => {
        impl<$($Ti),+> ValueList for ($($Ti,)+)
        where
            $($Ti: Value),+
        {
            fn serialized(&self) -> SerializedResult<'_> {
                let mut result = SerializedValues::new();
                $(
                    result.add_value(&self.$FieldI) ?;
                )*
                Ok(Cow::Owned(result))
            }
        }
    }
}

impl_value_list_for_tuple!(T0, T1; 0, 1);
impl_value_list_for_tuple!(T0, T1, T2; 0, 1, 2);
impl_value_list_for_tuple!(T0, T1, T2, T3; 0, 1, 2, 3);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4; 0, 1, 2, 3, 4);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5; 0, 1, 2, 3, 4, 5);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6; 0, 1, 2, 3, 4, 5, 6);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7; 0, 1, 2, 3, 4, 5, 6, 7);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8; 0, 1, 2, 3, 4, 5, 6, 7, 8);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9; 
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10; 
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
impl_value_list_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15;
                           0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

// Every &impl ValueList should also implement ValueList
impl<T: ValueList> ValueList for &T {
    fn serialized(&self) -> SerializedResult<'_> {
        <T as ValueList>::serialized(*self)
    }
}

impl ValueList for SerializedValues {
    fn serialized(&self) -> SerializedResult<'_> {
        Ok(Cow::Borrowed(self))
    }
}

impl<'b> ValueList for Cow<'b, SerializedValues> {
    fn serialized(&self) -> SerializedResult<'_> {
        Ok(Cow::Borrowed(self.as_ref()))
    }
}

impl<'b> ValueList for SerializedResult<'b> {
    fn serialized(&self) -> SerializedResult<'_> {
        match self {
            Ok(ser_values_cow) => Ok(Cow::Borrowed(ser_values_cow.as_ref())),
            Err(e) => Err(*e),
        }
    }
}

//
// BatchValues impls
//

// Implement BatchValues for arrays of ValueList types
impl<T: ValueList> BatchValues for &[T] {
    fn len(&self) -> usize {
        <[T]>::len(*self)
    }

    fn write_nth_to_request(
        &self,
        n: usize,
        buf: &mut impl BufMut,
    ) -> Result<(), SerializeValuesError> {
        self[n].write_to_request(buf)?;
        Ok(())
    }
}

// Here is an example implemetation for (T0, )
// Further variants are done using a macro
impl<T0: ValueList> BatchValues for (T0,) {
    fn len(&self) -> usize {
        1
    }

    fn write_nth_to_request(
        &self,
        n: usize,
        buf: &mut impl BufMut,
    ) -> Result<(), SerializeValuesError> {
        match n {
            0 => self.0.write_to_request(buf)?,
            _ => panic!("Tried to serialize ValueList with an out of range index!"),
        };

        Ok(())
    }
}

macro_rules! impl_batch_values_for_tuple {
    ( $($Ti:ident),* ; $($FieldI:tt),* ; $TupleSize:tt ) => {
        impl<$($Ti),+> BatchValues for ($($Ti,)+)
        where
        $($Ti: ValueList),+
        {
            fn len(&self) -> usize{
                $TupleSize
            }

            fn write_nth_to_request(&self, n: usize, buf: &mut impl BufMut) -> Result<(), SerializeValuesError> {
                match n {
                    $(
                        $FieldI => self.$FieldI.write_to_request(buf) ?,
                    )*
                    _ => panic!("Tried to serialize ValueList with an out of range index!"),
                }

                Ok(())
            }
        }
    }
}

impl_batch_values_for_tuple!(T0, T1; 0, 1; 2);
impl_batch_values_for_tuple!(T0, T1, T2; 0, 1, 2; 3);
impl_batch_values_for_tuple!(T0, T1, T2, T3; 0, 1, 2, 3; 4);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4; 0, 1, 2, 3, 4; 5);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5; 0, 1, 2, 3, 4, 5; 6);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6; 0, 1, 2, 3, 4, 5, 6; 7);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7; 0, 1, 2, 3, 4, 5, 6, 7; 8);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8; 0, 1, 2, 3, 4, 5, 6, 7, 8; 9);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9; 
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9; 10);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10; 
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10; 11);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11; 
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11; 12);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12; 
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12; 13);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13; 
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13; 14);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14; 
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14; 15);
impl_batch_values_for_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15;
                             0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15; 16);

// Every &impl BatchValues should also implement BatchValues
impl<T: BatchValues> BatchValues for &T {
    fn len(&self) -> usize {
        <T as BatchValues>::len(*self)
    }

    fn write_nth_to_request(
        &self,
        n: usize,
        buf: &mut impl BufMut,
    ) -> Result<(), SerializeValuesError> {
        <T as BatchValues>::write_nth_to_request(*self, n, buf)?;
        Ok(())
    }
}
