use bytes::Bytes;
use scylla::deserialize::FrameSlice;
use scylla::deserialize::value::DeserializeValue as DeserializeValueTrait;
use scylla::frame::response::result::{ColumnType, NativeType};
use scylla::serialize::value::SerializeValue as SerializeValueTrait;
use scylla::serialize::writers::CellWriter;
use scylla::{DeserializeValue, SerializeValue};

// --- TEST HELPERS ---

// Helper to avoid repeating serialization/deserialization logic
fn assert_round_trip<T>(
    value: T,
    typ: ColumnType,
    expected_bytes: &[u8], // Raw data (without length header)
) where
    T: SerializeValueTrait
        + for<'f, 'm> DeserializeValueTrait<'f, 'm>
        + PartialEq
        + std::fmt::Debug
        + Copy,
{
    // Serialization Test
    let mut data = Vec::new();
    let writer = CellWriter::new(&mut data);
    value.serialize(&typ, writer).unwrap();

    // Verify length header (4 bytes big endian) + data
    let len = expected_bytes.len() as i32;
    let mut expected_full = len.to_be_bytes().to_vec();
    expected_full.extend_from_slice(expected_bytes);

    assert_eq!(data, expected_full, "Serialization failed (byte mismatch)");

    // Deserialization Test
    // Simulate reading from a frame (skip 4 bytes length header)
    let payload = Bytes::copy_from_slice(expected_bytes);
    let slice = FrameSlice::new(&payload);

    let deserialized = T::deserialize(&typ, Some(slice)).expect("Deserialization failed");
    assert_eq!(
        value, deserialized,
        "Deserialized value does not match input"
    );
}

fn assert_deser_error<T>(typ: ColumnType, bad_bytes: &[u8])
where
    T: for<'f, 'm> DeserializeValueTrait<'f, 'm> + std::fmt::Debug,
{
    let payload = Bytes::copy_from_slice(bad_bytes);
    let slice = FrameSlice::new(&payload);
    let result = T::deserialize(&typ, Some(slice));
    assert!(
        result.is_err(),
        "Expected error, but deserialization succeeded: {:?}",
        result
    );
}

#[derive(SerializeValue, DeserializeValue, Debug, PartialEq, Copy, Clone)]
#[scylla(repr = "i32")]
enum DefaultInt {
    A = 0,
    B = 1,
    C = 100,
}

#[derive(SerializeValue, DeserializeValue, Debug, PartialEq, Copy, Clone)]
#[scylla(repr = "i8")]
enum TinyEnum {
    Small = 10,
    Max = 127,
}

#[derive(SerializeValue, DeserializeValue, Debug, PartialEq, Copy, Clone)]
#[scylla(repr = "i16")]
enum SmallEnum {
    Kilo = 1000,
    BigKilo = 30000,
}

#[derive(SerializeValue, DeserializeValue, Debug, PartialEq, Copy, Clone)]
#[scylla(repr = "i64")]
enum BigEnum {
    Big = 5_000_000_000,
}

#[derive(SerializeValue, DeserializeValue, Debug, PartialEq, Copy, Clone)]
#[scylla(repr = "i32")]
enum Gaps {
    First = 1,
    Jump = 5,
    FarAway = 99,
}

// --- TESTS ---

#[test]
fn test_i32_standard() {
    let typ = ColumnType::Native(NativeType::Int);

    // 0 -> [00, 00, 00, 00]
    assert_round_trip(DefaultInt::A, typ.clone(), &[0, 0, 0, 0]);
    // 100 -> [00, 00, 00, 64]
    assert_round_trip(DefaultInt::C, typ.clone(), &[0, 0, 0, 100]);
}

#[test]
fn test_i8_tinyint() {
    let typ = ColumnType::Native(NativeType::TinyInt);

    // 10 -> [0A]
    assert_round_trip(TinyEnum::Small, typ.clone(), &[10]);

    // 127 -> [7F] (Max i8)
    assert_round_trip(TinyEnum::Max, typ.clone(), &[0x7F]);
}

#[test]
fn test_i16_smallint() {
    let typ = ColumnType::Native(NativeType::SmallInt);

    // 1000 -> [03, E8]
    assert_round_trip(SmallEnum::Kilo, typ.clone(), &[0x03, 0xE8]);

    // 30000 -> [75, 30]
    assert_round_trip(SmallEnum::BigKilo, typ.clone(), &30000i16.to_be_bytes());
}

#[test]
fn test_i64_bigint() {
    let typ = ColumnType::Native(NativeType::BigInt);

    // 5 billion -> [00, 00, 00, 01, 2A, 05, F2, 00]
    assert_round_trip(BigEnum::Big, typ.clone(), &5_000_000_000i64.to_be_bytes());
}

#[test]
fn test_gaps_in_discriminants() {
    let typ = ColumnType::Native(NativeType::Int);

    assert_round_trip(Gaps::First, typ.clone(), &1i32.to_be_bytes());
    assert_round_trip(Gaps::Jump, typ.clone(), &5i32.to_be_bytes());
    assert_round_trip(Gaps::FarAway, typ.clone(), &99i32.to_be_bytes());

    // Test a "hole" (value 2 does not exist in the Enum)
    let bad_val = 2i32.to_be_bytes();
    assert_deser_error::<Gaps>(typ.clone(), &bad_val);
}

#[test]
fn test_invalid_data_errors() {
    let typ = ColumnType::Native(NativeType::Int);

    // Value out of range
    let out_of_range = 999i32.to_be_bytes();
    assert_deser_error::<DefaultInt>(typ.clone(), &out_of_range);

    // Wrong data length (3 bytes instead of 4 for Int)
    // This should fail at the underlying i32::deserialize level
    let too_short = vec![0, 0, 1];
    assert_deser_error::<DefaultInt>(typ.clone(), &too_short);
}

#[test]
fn test_type_check_mismatch() {
    // Verify that type_check works properly.
    // Trying to use an Enum (i32) on a TEXT column should fail.

    let bad_typ = ColumnType::Native(NativeType::Text);

    let check = <DefaultInt as DeserializeValueTrait>::type_check(&bad_typ);

    assert!(
        check.is_err(),
        "Should return TypeCheck error (Int vs Text)"
    );
}
