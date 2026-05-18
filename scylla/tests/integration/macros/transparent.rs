use bytes::Bytes;
use scylla::deserialize::FrameSlice;
use scylla::deserialize::value::DeserializeValue as DeserializeValueTrait;
use scylla::frame::response::result::{ColumnType, NativeType};
use scylla::serialize::value::SerializeValue as SerializeValueTrait;
use scylla::serialize::writers::CellWriter;
use scylla::{DeserializeValue, SerializeValue};

fn assert_round_trip<T>(
    value: T,
    typ: ColumnType,
    expected_bytes: &[u8], // Raw payload without length header
) where
    T: SerializeValueTrait
        + for<'f, 'm> DeserializeValueTrait<'f, 'm>
        + PartialEq
        + std::fmt::Debug
        + Copy,
{
    let mut data = Vec::new();
    let writer = CellWriter::new(&mut data);
    value.serialize(&typ, writer).unwrap();

    // Verify length header (4 bytes big endian) + payload
    let len = expected_bytes.len() as i32;
    let mut expected_full = len.to_be_bytes().to_vec();
    expected_full.extend_from_slice(expected_bytes);

    assert_eq!(data, expected_full, "Serialization failed (byte mismatch)");

    // Simulate reading from a frame (we skip the 4-byte length header)
    let payload = Bytes::copy_from_slice(expected_bytes);
    let slice = FrameSlice::new(&payload);

    let deserialized = T::deserialize(&typ, Some(slice)).expect("Deserialization failed");
    assert_eq!(
        value, deserialized,
        "Deserialized value does not match input"
    );
}

// Tuple Struct with a primitive inner type (i32)
#[derive(SerializeValue, DeserializeValue, PartialEq, Debug, Copy, Clone)]
#[scylla(transparent)]
struct TransparentTuple(i32);

// Named Struct with a primitive inner type (i32)
#[derive(SerializeValue, DeserializeValue, PartialEq, Debug, Copy, Clone)]
#[scylla(transparent)]
struct TransparentNamed {
    val: i32,
}

// Deeply nested transparent structs (Wrapper around Wrapper around i32)
#[derive(SerializeValue, DeserializeValue, PartialEq, Debug, Copy, Clone)]
#[scylla(transparent)]
struct Outer(Inner);

#[derive(SerializeValue, DeserializeValue, PartialEq, Debug, Copy, Clone)]
#[scylla(transparent)]
struct Inner(i32);

// A standard UDT struct that derives SerializeValue/DeserializeValue itself
#[derive(SerializeValue, DeserializeValue, PartialEq, Debug, Clone)]
struct MyUdt {
    a: i32,
    b: String,
}

// A transparent wrapper around the UDT
#[derive(SerializeValue, DeserializeValue, PartialEq, Debug, Clone)]
#[scylla(transparent)]
struct TransparentOverUdt(MyUdt);

#[test]
fn test_transparent_tuple_struct() {
    let typ = ColumnType::Native(NativeType::Int);
    let val = 12345i32;
    // 12345 in Big Endian hex is 0x00003039
    let expected_bytes = &val.to_be_bytes();

    assert_round_trip(TransparentTuple(val), typ, expected_bytes);
}

#[test]
fn test_transparent_named_struct() {
    let typ = ColumnType::Native(NativeType::Int);
    let val = -123i32;
    let expected_bytes = &val.to_be_bytes();

    assert_round_trip(TransparentNamed { val }, typ, expected_bytes);
}

#[test]
fn test_nested_transparent_structs() {
    let typ = ColumnType::Native(NativeType::Int);
    let val = 999i32;
    let expected_bytes = &val.to_be_bytes();

    // Outer(Inner(999)) should serialize exactly like i32(999)
    assert_round_trip(Outer(Inner(val)), typ, expected_bytes);
}

#[test]
fn test_type_check_delegation() {
    // This test verifies that type checking is correctly delegated to the inner type.
    // TransparentTuple wraps an i32. It should accept Int, but reject Text.

    let valid_type = ColumnType::Native(NativeType::Int);
    let invalid_type = ColumnType::Native(NativeType::Text);

    assert!(
        <TransparentTuple as DeserializeValueTrait>::type_check(&valid_type).is_ok(),
        "TransparentTuple should accept Int column type"
    );

    let err = <TransparentTuple as DeserializeValueTrait>::type_check(&invalid_type);
    assert!(
        err.is_err(),
        "TransparentTuple should reject Text column type (delegating to i32)"
    );
}

#[test]
fn test_deserialization_error_propagation() {
    // Verify that errors from the inner type are propagated correctly.
    // If we have less bytes than required for i32 (4 bytes), it should fail.

    let typ = ColumnType::Native(NativeType::Int);
    let bad_bytes = vec![0x00, 0x01]; // Only 2 bytes, i32 needs 4

    let payload = Bytes::copy_from_slice(&bad_bytes);
    let slice = FrameSlice::new(&payload);

    let result = <TransparentTuple as DeserializeValueTrait>::deserialize(&typ, Some(slice));

    assert!(
        result.is_err(),
        "Deserialization should fail when underlying data is invalid for inner type"
    );
}

#[test]
fn test_transparent_udt_wrapper() {
    use scylla::frame::response::result::UserDefinedType;
    use std::sync::Arc;

    let udt_def = UserDefinedType {
        keyspace: "ks".into(),
        name: "my_udt".into(),
        field_types: vec![
            ("a".into(), ColumnType::Native(NativeType::Int)),
            ("b".into(), ColumnType::Native(NativeType::Text)),
        ],
    };

    let typ = ColumnType::UserDefinedType {
        frozen: false,
        definition: Arc::new(udt_def),
    };

    let inner = MyUdt {
        a: 123,
        b: "test_string".to_string(),
    };
    let wrapper = TransparentOverUdt(inner.clone());

    let mut data_inner = Vec::new();
    let writer_inner = CellWriter::new(&mut data_inner);
    SerializeValueTrait::serialize(&inner, &typ, writer_inner).unwrap();

    let mut data_wrapper = Vec::new();
    let writer_wrapper = CellWriter::new(&mut data_wrapper);
    SerializeValueTrait::serialize(&wrapper, &typ, writer_wrapper).unwrap();

    assert_eq!(
        data_inner, data_wrapper,
        "Transparent wrapper serialized differently than the inner type"
    );

    assert!(
        data_inner.len() > 4,
        "Serialized data is too short to contain a length header"
    );
    let payload = Bytes::copy_from_slice(&data_inner[4..]);
    let slice = FrameSlice::new(&payload);

    let deserialized_wrapper =
        <TransparentOverUdt as DeserializeValueTrait>::deserialize(&typ, Some(slice))
            .expect("Failed to deserialize wrapper from inner bytes");

    assert_eq!(deserialized_wrapper.0, inner);
}
