use std::net::IpAddr;
use std::result::Result as StdResult;

use thiserror::Error;
use uuid::Uuid;

use crate::deserialize::value::DeserializeValue;
use crate::deserialize::value::{
    mk_deser_err, BuiltinDeserializationErrorKind, MapIterator, UdtIterator,
};
use crate::deserialize::DeserializationError;
use crate::deserialize::FrameSlice;
use crate::frame::response::result::{CollectionType, ColumnType};
use crate::frame::types;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[error("Value is too large to fit in the CQL type")]
pub struct ValueOverflow;

/// Represents an unset value
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Unset;

/// Represents an counter value
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Counter(pub i64);

/// Enum providing a way to represent a value that might be unset
#[derive(Debug, Clone, Copy, Default)]
pub enum MaybeUnset<V> {
    #[default]
    Unset,
    Set(V),
}

impl<V> MaybeUnset<V> {
    #[inline]
    pub fn from_option(opt: Option<V>) -> Self {
        match opt {
            Some(v) => Self::Set(v),
            None => Self::Unset,
        }
    }
}

/// Represents timeuuid (uuid V1) value
///
/// This type has custom comparison logic which follows Scylla/Cassandra semantics.
/// For details, see [`Ord` implementation](#impl-Ord-for-CqlTimeuuid).
#[derive(Debug, Clone, Copy, Eq)]
pub struct CqlTimeuuid(Uuid);

/// [`Uuid`] delegate methods
impl CqlTimeuuid {
    pub fn nil() -> Self {
        Self(Uuid::nil())
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    pub fn as_u128(&self) -> u128 {
        self.0.as_u128()
    }

    pub fn as_fields(&self) -> (u32, u16, u16, &[u8; 8]) {
        self.0.as_fields()
    }

    pub fn as_u64_pair(&self) -> (u64, u64) {
        self.0.as_u64_pair()
    }

    pub fn from_slice(b: &[u8]) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::from_slice(b)?))
    }

    pub fn from_slice_le(b: &[u8]) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::from_slice_le(b)?))
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    pub fn from_bytes_le(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes_le(bytes))
    }

    pub fn from_fields(d1: u32, d2: u16, d3: u16, d4: &[u8; 8]) -> Self {
        Self(Uuid::from_fields(d1, d2, d3, d4))
    }

    pub fn from_fields_le(d1: u32, d2: u16, d3: u16, d4: &[u8; 8]) -> Self {
        Self(Uuid::from_fields_le(d1, d2, d3, d4))
    }

    pub fn from_u128(v: u128) -> Self {
        Self(Uuid::from_u128(v))
    }

    pub fn from_u128_le(v: u128) -> Self {
        Self(Uuid::from_u128_le(v))
    }

    pub fn from_u64_pair(high_bits: u64, low_bits: u64) -> Self {
        Self(Uuid::from_u64_pair(high_bits, low_bits))
    }
}

impl CqlTimeuuid {
    /// Read 8 most significant bytes of timeuuid from serialized bytes
    fn msb(&self) -> u64 {
        // Scylla and Cassandra use a standard UUID memory layout for MSB:
        // 4 bytes    2 bytes    2 bytes
        // time_low - time_mid - time_hi_and_version
        let bytes = self.0.as_bytes();
        u64::from_be_bytes([
            bytes[6] & 0x0f,
            bytes[7],
            bytes[4],
            bytes[5],
            bytes[0],
            bytes[1],
            bytes[2],
            bytes[3],
        ])
    }

    fn lsb(&self) -> u64 {
        let bytes = self.0.as_bytes();
        u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ])
    }

    fn lsb_signed(&self) -> u64 {
        self.lsb() ^ 0x8080808080808080
    }
}

impl std::str::FromStr for CqlTimeuuid {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::from_str(s)?))
    }
}

impl std::fmt::Display for CqlTimeuuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<Uuid> for CqlTimeuuid {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}

impl From<CqlTimeuuid> for Uuid {
    fn from(value: CqlTimeuuid) -> Self {
        value.0
    }
}

impl From<Uuid> for CqlTimeuuid {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

/// Compare two values of timeuuid type.
///
/// Cassandra legacy requires:
/// - converting 8 most significant bytes to date, which is then compared.
/// - masking off UUID version from the 8 ms-bytes during compare, to
///   treat possible non-version-1 UUID the same way as UUID.
/// - using signed compare for least significant bits.
impl Ord for CqlTimeuuid {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let mut res = self.msb().cmp(&other.msb());
        if let std::cmp::Ordering::Equal = res {
            res = self.lsb_signed().cmp(&other.lsb_signed());
        }
        res
    }
}

impl PartialOrd for CqlTimeuuid {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for CqlTimeuuid {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl std::hash::Hash for CqlTimeuuid {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.lsb_signed().hash(state);
        self.msb().hash(state);
    }
}

/// Native CQL `varint` representation.
///
/// Represented as two's-complement binary in big-endian order.
///
/// This type is a raw representation in bytes. It's the default
/// implementation of `varint` type - independent of any
/// external crates and crate features.
///
/// The type is not very useful in most use cases.
/// However, users can make use of more complex types
/// such as `num_bigint::BigInt` (v0.3/v0.4).
/// The library support (e.g. conversion from [`CqlValue`]) for these types is
/// enabled via `num-bigint-03` and `num-bigint-04` crate features.
///
/// This struct holds owned bytes. If you wish to borrow the bytes instead,
/// see [`CqlVarintBorrowed`] documentation.
///
/// # DB data format
/// Notice that [constructors](CqlVarint#impl-CqlVarint)
/// don't perform any normalization on the provided data.
/// This means that underlying bytes may contain leading zeros.
///
/// Currently, Scylla and Cassandra support non-normalized `varint` values.
/// Bytes provided by the user via constructor are passed to DB as is.
///
/// The implementation of [`PartialEq`], however, normalizes the underlying bytes
/// before comparison. For details, check [examples](#impl-PartialEq-for-CqlVarint).
#[derive(Clone, Eq, Debug)]
pub struct CqlVarint(Vec<u8>);

/// A borrowed version of native CQL `varint` representation.
///
/// Refer to the documentation of [`CqlVarint`].
/// Especially, see the disclaimer about [non-normalized values](CqlVarint#db-data-format).
#[derive(Clone, Eq, Debug)]
pub struct CqlVarintBorrowed<'b>(&'b [u8]);

/// Constructors from bytes
impl CqlVarint {
    /// Creates a [`CqlVarint`] from an array of bytes in
    /// two's complement big-endian binary representation.
    ///
    /// See: disclaimer about [non-normalized values](CqlVarint#db-data-format).
    pub fn from_signed_bytes_be(digits: Vec<u8>) -> Self {
        Self(digits)
    }

    /// Creates a [`CqlVarint`] from a slice of bytes in
    /// two's complement binary big-endian representation.
    ///
    /// See: disclaimer about [non-normalized values](CqlVarint#db-data-format).
    pub fn from_signed_bytes_be_slice(digits: &[u8]) -> Self {
        Self::from_signed_bytes_be(digits.to_vec())
    }
}

/// Constructors from bytes
impl<'b> CqlVarintBorrowed<'b> {
    /// Creates a [`CqlVarintBorrowed`] from a slice of bytes in
    /// two's complement binary big-endian representation.
    ///
    /// See: disclaimer about [non-normalized values](CqlVarint#db-data-format).
    pub fn from_signed_bytes_be_slice(digits: &'b [u8]) -> Self {
        Self(digits)
    }
}

/// Conversion to bytes
impl CqlVarint {
    /// Converts [`CqlVarint`] to an array of bytes in two's
    /// complement binary big-endian representation.
    pub fn into_signed_bytes_be(self) -> Vec<u8> {
        self.0
    }

    /// Returns a slice of bytes in two's complement
    /// binary big-endian representation.
    pub fn as_signed_bytes_be_slice(&self) -> &[u8] {
        &self.0
    }
}

/// Conversion to bytes
impl CqlVarintBorrowed<'_> {
    /// Returns a slice of bytes in two's complement
    /// binary big-endian representation.
    pub fn as_signed_bytes_be_slice(&self) -> &[u8] {
        self.0
    }
}

/// An internal utility trait used to implement [`AsNormalizedVarintSlice`]
/// for both [`CqlVarint`] and [`CqlVarintBorrowed`].
trait AsVarintSlice {
    fn as_slice(&self) -> &[u8];
}
impl AsVarintSlice for CqlVarint {
    fn as_slice(&self) -> &[u8] {
        self.as_signed_bytes_be_slice()
    }
}
impl AsVarintSlice for CqlVarintBorrowed<'_> {
    fn as_slice(&self) -> &[u8] {
        self.as_signed_bytes_be_slice()
    }
}

/// An internal utility trait used to implement [`PartialEq`] and [`std::hash::Hash`]
/// for [`CqlVarint`] and [`CqlVarintBorrowed`].
trait AsNormalizedVarintSlice {
    fn as_normalized_slice(&self) -> &[u8];
}
impl<V: AsVarintSlice> AsNormalizedVarintSlice for V {
    fn as_normalized_slice(&self) -> &[u8] {
        let digits = self.as_slice();
        if digits.is_empty() {
            // num-bigint crate normalizes empty vector to 0.
            // We will follow the same approach.
            return &[0];
        }

        let non_zero_position = match digits.iter().position(|b| *b != 0) {
            Some(pos) => pos,
            None => {
                // Vector is filled with zeros. Represent it as 0.
                return &[0];
            }
        };

        if non_zero_position > 0 {
            // There were some leading zeros.
            // Now, there are two cases:
            let zeros_to_remove = if digits[non_zero_position] > 0x7f {
                // Most significant bit is 1, so we need to include one of the leading
                // zeros as originally it represented a positive number.
                non_zero_position - 1
            } else {
                // Most significant bit is 0 - positive number with no leading zeros.
                non_zero_position
            };
            return &digits[zeros_to_remove..];
        }

        // There were no leading zeros at all - leave as is.
        digits
    }
}

/// Compares two [`CqlVarint`] values after normalization.
///
/// # Example
///
/// ```rust
/// # use scylla_cql::value::CqlVarint;
/// let non_normalized_bytes = vec![0x00, 0x01];
/// let normalized_bytes = vec![0x01];
/// assert_eq!(
///     CqlVarint::from_signed_bytes_be(non_normalized_bytes),
///     CqlVarint::from_signed_bytes_be(normalized_bytes)
/// );
/// ```
impl PartialEq for CqlVarint {
    fn eq(&self, other: &Self) -> bool {
        self.as_normalized_slice() == other.as_normalized_slice()
    }
}

/// Computes the hash of normalized [`CqlVarint`].
impl std::hash::Hash for CqlVarint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_normalized_slice().hash(state)
    }
}

/// Compares two [`CqlVarintBorrowed`] values after normalization.
///
/// # Example
///
/// ```rust
/// # use scylla_cql::value::CqlVarintBorrowed;
/// let non_normalized_bytes = &[0x00, 0x01];
/// let normalized_bytes = &[0x01];
/// assert_eq!(
///     CqlVarintBorrowed::from_signed_bytes_be_slice(non_normalized_bytes),
///     CqlVarintBorrowed::from_signed_bytes_be_slice(normalized_bytes)
/// );
/// ```
impl PartialEq for CqlVarintBorrowed<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_normalized_slice() == other.as_normalized_slice()
    }
}

/// Computes the hash of normalized [`CqlVarintBorrowed`].
impl std::hash::Hash for CqlVarintBorrowed<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_normalized_slice().hash(state)
    }
}

#[cfg(feature = "num-bigint-03")]
impl From<num_bigint_03::BigInt> for CqlVarint {
    fn from(value: num_bigint_03::BigInt) -> Self {
        Self(value.to_signed_bytes_be())
    }
}

#[cfg(feature = "num-bigint-03")]
impl From<CqlVarint> for num_bigint_03::BigInt {
    fn from(val: CqlVarint) -> Self {
        num_bigint_03::BigInt::from_signed_bytes_be(&val.0)
    }
}

#[cfg(feature = "num-bigint-03")]
impl From<CqlVarintBorrowed<'_>> for num_bigint_03::BigInt {
    fn from(val: CqlVarintBorrowed<'_>) -> Self {
        num_bigint_03::BigInt::from_signed_bytes_be(val.0)
    }
}

#[cfg(feature = "num-bigint-04")]
impl From<num_bigint_04::BigInt> for CqlVarint {
    fn from(value: num_bigint_04::BigInt) -> Self {
        Self(value.to_signed_bytes_be())
    }
}

#[cfg(feature = "num-bigint-04")]
impl From<CqlVarint> for num_bigint_04::BigInt {
    fn from(val: CqlVarint) -> Self {
        num_bigint_04::BigInt::from_signed_bytes_be(&val.0)
    }
}

#[cfg(feature = "num-bigint-04")]
impl From<CqlVarintBorrowed<'_>> for num_bigint_04::BigInt {
    fn from(val: CqlVarintBorrowed<'_>) -> Self {
        num_bigint_04::BigInt::from_signed_bytes_be(val.0)
    }
}

/// Native CQL `decimal` representation.
///
/// Represented as a pair:
/// - a [`CqlVarint`] value
/// - 32-bit integer which determines the position of the decimal point
///
/// This struct holds owned bytes. If you wish to borrow the bytes instead,
/// see [`CqlDecimalBorrowed`] documentation.
///
/// The type is not very useful in most use cases.
/// However, users can make use of more complex types
/// such as `bigdecimal::BigDecimal` (v0.4).
/// The library support (e.g. conversion from [`CqlValue`]) for the type is
/// enabled via `bigdecimal-04` crate feature.
///
/// # DB data format
/// Notice that [constructors](CqlDecimal#impl-CqlDecimal)
/// don't perform any normalization on the provided data.
/// For more details, see [`CqlVarint`] documentation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CqlDecimal {
    int_val: CqlVarint,
    scale: i32,
}

/// Borrowed version of native CQL `decimal` representation.
///
/// Represented as a pair:
/// - a [`CqlVarintBorrowed`] value
/// - 32-bit integer which determines the position of the decimal point
///
/// Refer to the documentation of [`CqlDecimal`].
/// Especially, see the disclaimer about [non-normalized values](CqlDecimal#db-data-format).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CqlDecimalBorrowed<'b> {
    int_val: CqlVarintBorrowed<'b>,
    scale: i32,
}

/// Constructors
impl CqlDecimal {
    /// Creates a [`CqlDecimal`] from an array of bytes
    /// representing [`CqlVarint`] and a 32-bit scale.
    ///
    /// See: disclaimer about [non-normalized values](CqlVarint#db-data-format).
    pub fn from_signed_be_bytes_and_exponent(bytes: Vec<u8>, scale: i32) -> Self {
        Self {
            int_val: CqlVarint::from_signed_bytes_be(bytes),
            scale,
        }
    }

    /// Creates a [`CqlDecimal`] from a slice of bytes
    /// representing [`CqlVarint`] and a 32-bit scale.
    ///
    /// See: disclaimer about [non-normalized values](CqlVarint#db-data-format).
    pub fn from_signed_be_bytes_slice_and_exponent(bytes: &[u8], scale: i32) -> Self {
        Self::from_signed_be_bytes_and_exponent(bytes.to_vec(), scale)
    }
}

/// Constructors
impl<'b> CqlDecimalBorrowed<'b> {
    /// Creates a [`CqlDecimalBorrowed`] from a slice of bytes
    /// representing [`CqlVarintBorrowed`] and a 32-bit scale.
    ///
    /// See: disclaimer about [non-normalized values](CqlVarint#db-data-format).
    pub fn from_signed_be_bytes_slice_and_exponent(bytes: &'b [u8], scale: i32) -> Self {
        Self {
            int_val: CqlVarintBorrowed::from_signed_bytes_be_slice(bytes),
            scale,
        }
    }
}

/// Conversion to raw bytes
impl CqlDecimal {
    /// Returns a slice of bytes in two's complement
    /// binary big-endian representation and a scale.
    pub fn as_signed_be_bytes_slice_and_exponent(&self) -> (&[u8], i32) {
        (self.int_val.as_signed_bytes_be_slice(), self.scale)
    }

    /// Converts [`CqlDecimal`] to an array of bytes in two's
    /// complement binary big-endian representation and a scale.
    pub fn into_signed_be_bytes_and_exponent(self) -> (Vec<u8>, i32) {
        (self.int_val.into_signed_bytes_be(), self.scale)
    }
}

/// Conversion to raw bytes
impl CqlDecimalBorrowed<'_> {
    /// Returns a slice of bytes in two's complement
    /// binary big-endian representation and a scale.
    pub fn as_signed_be_bytes_slice_and_exponent(&self) -> (&[u8], i32) {
        (self.int_val.as_signed_bytes_be_slice(), self.scale)
    }
}

#[cfg(feature = "bigdecimal-04")]
impl From<CqlDecimal> for bigdecimal_04::BigDecimal {
    fn from(value: CqlDecimal) -> Self {
        Self::from((
            bigdecimal_04::num_bigint::BigInt::from_signed_bytes_be(
                value.int_val.as_signed_bytes_be_slice(),
            ),
            value.scale as i64,
        ))
    }
}

#[cfg(feature = "bigdecimal-04")]
impl From<CqlDecimalBorrowed<'_>> for bigdecimal_04::BigDecimal {
    fn from(value: CqlDecimalBorrowed) -> Self {
        Self::from((
            bigdecimal_04::num_bigint::BigInt::from_signed_bytes_be(
                value.int_val.as_signed_bytes_be_slice(),
            ),
            value.scale as i64,
        ))
    }
}

#[cfg(feature = "bigdecimal-04")]
impl TryFrom<bigdecimal_04::BigDecimal> for CqlDecimal {
    type Error = <i64 as TryInto<i32>>::Error;

    fn try_from(value: bigdecimal_04::BigDecimal) -> Result<Self, Self::Error> {
        let (bigint, scale) = value.into_bigint_and_exponent();
        let bytes = bigint.to_signed_bytes_be();
        Ok(Self::from_signed_be_bytes_and_exponent(
            bytes,
            scale.try_into()?,
        ))
    }
}

/// Native CQL date representation that allows for a bigger range of dates (-262145-1-1 to 262143-12-31).
///
/// Represented as number of days since -5877641-06-23 i.e. 2^31 days before unix epoch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CqlDate(pub u32);

/// Native CQL timestamp representation that allows full supported timestamp range.
///
/// Represented as signed milliseconds since unix epoch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CqlTimestamp(pub i64);

/// Native CQL time representation.
///
/// Represented as nanoseconds since midnight.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CqlTime(pub i64);

impl CqlDate {
    fn try_to_chrono_04_naive_date(&self) -> Result<chrono_04::NaiveDate, ValueOverflow> {
        let days_since_unix_epoch = self.0 as i64 - (1 << 31);

        // date_days is u32 then converted to i64 then we subtract 2^31;
        // Max value is 2^31, min value is -2^31. Both values can safely fit in chrono::Duration, this call won't panic
        let duration_since_unix_epoch =
            chrono_04::Duration::try_days(days_since_unix_epoch).unwrap();

        chrono_04::NaiveDate::from_yo_opt(1970, 1)
            .unwrap()
            .checked_add_signed(duration_since_unix_epoch)
            .ok_or(ValueOverflow)
    }
}

#[cfg(feature = "chrono-04")]
impl From<chrono_04::NaiveDate> for CqlDate {
    fn from(value: chrono_04::NaiveDate) -> Self {
        let unix_epoch = chrono_04::NaiveDate::from_yo_opt(1970, 1).unwrap();

        // `NaiveDate` range is -262145-01-01 to 262143-12-31
        // Both values are well within supported range
        let days = ((1 << 31) + value.signed_duration_since(unix_epoch).num_days()) as u32;

        Self(days)
    }
}

#[cfg(feature = "chrono-04")]
impl TryInto<chrono_04::NaiveDate> for CqlDate {
    type Error = ValueOverflow;

    fn try_into(self) -> Result<chrono_04::NaiveDate, Self::Error> {
        self.try_to_chrono_04_naive_date()
    }
}

impl CqlTimestamp {
    fn try_to_chrono_04_datetime_utc(
        &self,
    ) -> Result<chrono_04::DateTime<chrono_04::Utc>, ValueOverflow> {
        use chrono_04::TimeZone;
        match chrono_04::Utc.timestamp_millis_opt(self.0) {
            chrono_04::LocalResult::Single(datetime) => Ok(datetime),
            _ => Err(ValueOverflow),
        }
    }
}

#[cfg(feature = "chrono-04")]
impl From<chrono_04::DateTime<chrono_04::Utc>> for CqlTimestamp {
    fn from(value: chrono_04::DateTime<chrono_04::Utc>) -> Self {
        Self(value.timestamp_millis())
    }
}

#[cfg(feature = "chrono-04")]
impl TryInto<chrono_04::DateTime<chrono_04::Utc>> for CqlTimestamp {
    type Error = ValueOverflow;

    fn try_into(self) -> Result<chrono_04::DateTime<chrono_04::Utc>, Self::Error> {
        self.try_to_chrono_04_datetime_utc()
    }
}

#[cfg(feature = "chrono-04")]
impl TryFrom<chrono_04::NaiveTime> for CqlTime {
    type Error = ValueOverflow;

    fn try_from(value: chrono_04::NaiveTime) -> Result<Self, Self::Error> {
        let nanos = value
            .signed_duration_since(chrono_04::NaiveTime::MIN)
            .num_nanoseconds()
            .unwrap();

        // Value can exceed max CQL time in case of leap second
        if nanos <= 86399999999999 {
            Ok(Self(nanos))
        } else {
            Err(ValueOverflow)
        }
    }
}

#[cfg(feature = "chrono-04")]
impl TryInto<chrono_04::NaiveTime> for CqlTime {
    type Error = ValueOverflow;

    fn try_into(self) -> Result<chrono_04::NaiveTime, Self::Error> {
        let secs = (self.0 / 1_000_000_000)
            .try_into()
            .map_err(|_| ValueOverflow)?;
        let nanos = (self.0 % 1_000_000_000)
            .try_into()
            .map_err(|_| ValueOverflow)?;
        chrono_04::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos).ok_or(ValueOverflow)
    }
}

#[cfg(feature = "time-03")]
impl From<time_03::Date> for CqlDate {
    fn from(value: time_03::Date) -> Self {
        const JULIAN_DAY_OFFSET: i64 =
            (1 << 31) - time_03::OffsetDateTime::UNIX_EPOCH.date().to_julian_day() as i64;

        // Statically assert that no possible value will ever overflow
        const _: () = assert!(
            time_03::Date::MAX.to_julian_day() as i64 + JULIAN_DAY_OFFSET < u32::MAX as i64
        );
        const _: () = assert!(
            time_03::Date::MIN.to_julian_day() as i64 + JULIAN_DAY_OFFSET > u32::MIN as i64
        );

        let days = value.to_julian_day() as i64 + JULIAN_DAY_OFFSET;

        Self(days as u32)
    }
}

#[cfg(feature = "time-03")]
impl TryInto<time_03::Date> for CqlDate {
    type Error = ValueOverflow;

    fn try_into(self) -> Result<time_03::Date, Self::Error> {
        const JULIAN_DAY_OFFSET: i64 =
            (1 << 31) - time_03::OffsetDateTime::UNIX_EPOCH.date().to_julian_day() as i64;

        let julian_days = (self.0 as i64 - JULIAN_DAY_OFFSET)
            .try_into()
            .map_err(|_| ValueOverflow)?;

        time_03::Date::from_julian_day(julian_days).map_err(|_| ValueOverflow)
    }
}

#[cfg(feature = "time-03")]
impl From<time_03::OffsetDateTime> for CqlTimestamp {
    fn from(value: time_03::OffsetDateTime) -> Self {
        // Statically assert that no possible value will ever overflow. OffsetDateTime doesn't allow offset to overflow
        // the UTC PrimitiveDateTime value value
        const _: () = assert!(
            time_03::PrimitiveDateTime::MAX
                .assume_utc()
                .unix_timestamp_nanos()
                // Nanos to millis
                / 1_000_000
                < i64::MAX as i128
        );
        const _: () = assert!(
            time_03::PrimitiveDateTime::MIN
                .assume_utc()
                .unix_timestamp_nanos()
                / 1_000_000
                > i64::MIN as i128
        );

        // Edge cases were statically asserted above, checked math is not required
        Self(value.unix_timestamp() * 1000 + value.millisecond() as i64)
    }
}

#[cfg(feature = "time-03")]
impl TryInto<time_03::OffsetDateTime> for CqlTimestamp {
    type Error = ValueOverflow;

    fn try_into(self) -> Result<time_03::OffsetDateTime, Self::Error> {
        time_03::OffsetDateTime::from_unix_timestamp_nanos(self.0 as i128 * 1_000_000)
            .map_err(|_| ValueOverflow)
    }
}

#[cfg(feature = "time-03")]
impl From<time_03::Time> for CqlTime {
    fn from(value: time_03::Time) -> Self {
        let (h, m, s, n) = value.as_hms_nano();

        // no need for checked arithmetic as all these types are guaranteed to fit in i64 without overflow
        let nanos = (h as i64 * 3600 + m as i64 * 60 + s as i64) * 1_000_000_000 + n as i64;

        Self(nanos)
    }
}

#[cfg(feature = "time-03")]
impl TryInto<time_03::Time> for CqlTime {
    type Error = ValueOverflow;

    fn try_into(self) -> Result<time_03::Time, Self::Error> {
        let h = self.0 / 3_600_000_000_000;
        let m = self.0 / 60_000_000_000 % 60;
        let s = self.0 / 1_000_000_000 % 60;
        let n = self.0 % 1_000_000_000;

        time_03::Time::from_hms_nano(
            h.try_into().map_err(|_| ValueOverflow)?,
            m as u8,
            s as u8,
            n as u32,
        )
        .map_err(|_| ValueOverflow)
    }
}

/// Represents a CQL Duration value
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub struct CqlDuration {
    pub months: i32,
    pub days: i32,
    pub nanoseconds: i64,
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum CqlValue {
    Ascii(String),
    Boolean(bool),
    Blob(Vec<u8>),
    Counter(Counter),
    Decimal(CqlDecimal),
    /// Days since -5877641-06-23 i.e. 2^31 days before unix epoch
    /// Can be converted to chrono::NaiveDate (-262145-1-1 to 262143-12-31) using as_date
    Date(CqlDate),
    Double(f64),
    Duration(CqlDuration),
    Empty,
    Float(f32),
    Int(i32),
    BigInt(i64),
    Text(String),
    /// Milliseconds since unix epoch
    Timestamp(CqlTimestamp),
    Inet(IpAddr),
    List(Vec<CqlValue>),
    Map(Vec<(CqlValue, CqlValue)>),
    Set(Vec<CqlValue>),
    UserDefinedType {
        keyspace: String,
        name: String,
        /// Order of `fields` vector must match the order of fields as defined in the UDT. The
        /// driver does not check it by itself, so incorrect data will be written if the order is
        /// wrong.
        fields: Vec<(String, Option<CqlValue>)>,
    },
    SmallInt(i16),
    TinyInt(i8),
    /// Nanoseconds since midnight
    Time(CqlTime),
    Timeuuid(CqlTimeuuid),
    Tuple(Vec<Option<CqlValue>>),
    Uuid(Uuid),
    Varint(CqlVarint),
    Vector(Vec<CqlValue>),
}

impl CqlValue {
    pub fn as_ascii(&self) -> Option<&String> {
        match self {
            Self::Ascii(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_cql_date(&self) -> Option<CqlDate> {
        match self {
            Self::Date(d) => Some(*d),
            _ => None,
        }
    }

    #[cfg(test)]
    #[cfg(feature = "chrono-04")]
    pub(crate) fn as_naive_date_04(&self) -> Option<chrono_04::NaiveDate> {
        self.as_cql_date().and_then(|date| date.try_into().ok())
    }

    #[cfg(test)]
    #[cfg(feature = "time-03")]
    pub(crate) fn as_date_03(&self) -> Option<time_03::Date> {
        self.as_cql_date().and_then(|date| date.try_into().ok())
    }

    pub fn as_cql_timestamp(&self) -> Option<CqlTimestamp> {
        match self {
            Self::Timestamp(i) => Some(*i),
            _ => None,
        }
    }

    #[cfg(test)]
    #[cfg(feature = "chrono-04")]
    pub(crate) fn as_datetime_04(&self) -> Option<chrono_04::DateTime<chrono_04::Utc>> {
        self.as_cql_timestamp().and_then(|ts| ts.try_into().ok())
    }

    #[cfg(test)]
    #[cfg(feature = "time-03")]
    pub(crate) fn as_offset_date_time_03(&self) -> Option<time_03::OffsetDateTime> {
        self.as_cql_timestamp().and_then(|ts| ts.try_into().ok())
    }

    pub fn as_cql_time(&self) -> Option<CqlTime> {
        match self {
            Self::Time(i) => Some(*i),
            _ => None,
        }
    }

    #[cfg(test)]
    #[cfg(feature = "chrono-04")]
    pub(crate) fn as_naive_time_04(&self) -> Option<chrono_04::NaiveTime> {
        self.as_cql_time().and_then(|ts| ts.try_into().ok())
    }

    #[cfg(test)]
    #[cfg(feature = "time-03")]
    pub(crate) fn as_time_03(&self) -> Option<time_03::Time> {
        self.as_cql_time().and_then(|ts| ts.try_into().ok())
    }

    pub fn as_cql_duration(&self) -> Option<CqlDuration> {
        match self {
            Self::Duration(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_counter(&self) -> Option<Counter> {
        match self {
            Self::Counter(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Self::Boolean(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<f64> {
        match self {
            Self::Double(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_uuid(&self) -> Option<Uuid> {
        match self {
            Self::Uuid(u) => Some(*u),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f32> {
        match self {
            Self::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i32> {
        match self {
            Self::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_bigint(&self) -> Option<i64> {
        match self {
            Self::BigInt(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_tinyint(&self) -> Option<i8> {
        match self {
            Self::TinyInt(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_smallint(&self) -> Option<i16> {
        match self {
            Self::SmallInt(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&Vec<u8>> {
        match self {
            Self::Blob(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&String> {
        match self {
            Self::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_timeuuid(&self) -> Option<CqlTimeuuid> {
        match self {
            Self::Timeuuid(u) => Some(*u),
            _ => None,
        }
    }

    pub fn into_string(self) -> Option<String> {
        match self {
            Self::Ascii(s) => Some(s),
            Self::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_blob(self) -> Option<Vec<u8>> {
        match self {
            Self::Blob(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_inet(&self) -> Option<IpAddr> {
        match self {
            Self::Inet(a) => Some(*a),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&Vec<CqlValue>> {
        match self {
            Self::List(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_set(&self) -> Option<&Vec<CqlValue>> {
        match self {
            Self::Set(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&Vec<(CqlValue, CqlValue)>> {
        match self {
            Self::Map(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_udt(&self) -> Option<&Vec<(String, Option<CqlValue>)>> {
        match self {
            Self::UserDefinedType { fields, .. } => Some(fields),
            _ => None,
        }
    }

    pub fn into_vec(self) -> Option<Vec<CqlValue>> {
        match self {
            Self::List(s) => Some(s),
            Self::Set(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_pair_vec(self) -> Option<Vec<(CqlValue, CqlValue)>> {
        match self {
            Self::Map(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_udt_pair_vec(self) -> Option<Vec<(String, Option<CqlValue>)>> {
        match self {
            Self::UserDefinedType { fields, .. } => Some(fields),
            _ => None,
        }
    }

    pub fn into_cql_varint(self) -> Option<CqlVarint> {
        match self {
            Self::Varint(i) => Some(i),
            _ => None,
        }
    }

    pub fn into_cql_decimal(self) -> Option<CqlDecimal> {
        match self {
            Self::Decimal(i) => Some(i),
            _ => None,
        }
    }
    // TODO
}

/// Displays a CqlValue. The syntax should resemble the CQL literal syntax
/// (but no guarantee is given that it's always the same).
impl std::fmt::Display for CqlValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use crate::pretty::{
            CqlStringLiteralDisplayer, HexBytes, MaybeNullDisplayer, PairDisplayer,
        };
        use itertools::Itertools;

        match self {
            // Scalar types
            CqlValue::Ascii(a) => write!(f, "{}", CqlStringLiteralDisplayer(a))?,
            CqlValue::Text(t) => write!(f, "{}", CqlStringLiteralDisplayer(t))?,
            CqlValue::Blob(b) => write!(f, "0x{:x}", HexBytes(b))?,
            CqlValue::Empty => write!(f, "0x")?,
            CqlValue::Decimal(d) => {
                let (bytes, scale) = d.as_signed_be_bytes_slice_and_exponent();
                write!(
                    f,
                    "blobAsDecimal(0x{:x}{:x})",
                    HexBytes(&scale.to_be_bytes()),
                    HexBytes(bytes)
                )?
            }
            CqlValue::Float(fl) => write!(f, "{}", fl)?,
            CqlValue::Double(d) => write!(f, "{}", d)?,
            CqlValue::Boolean(b) => write!(f, "{}", b)?,
            CqlValue::Int(i) => write!(f, "{}", i)?,
            CqlValue::BigInt(bi) => write!(f, "{}", bi)?,
            CqlValue::Inet(i) => write!(f, "'{}'", i)?,
            CqlValue::SmallInt(si) => write!(f, "{}", si)?,
            CqlValue::TinyInt(ti) => write!(f, "{}", ti)?,
            CqlValue::Varint(vi) => write!(
                f,
                "blobAsVarint(0x{:x})",
                HexBytes(vi.as_signed_bytes_be_slice())
            )?,
            CqlValue::Counter(c) => write!(f, "{}", c.0)?,
            CqlValue::Date(d) => {
                // TODO: chrono::NaiveDate does not handle the whole range
                // supported by the `date` datatype
                match d.try_to_chrono_04_naive_date() {
                    Ok(d) => write!(f, "'{}'", d)?,
                    Err(_) => f.write_str("<date out of representable range>")?,
                }
            }
            CqlValue::Duration(d) => write!(f, "{}mo{}d{}ns", d.months, d.days, d.nanoseconds)?,
            CqlValue::Time(CqlTime(t)) => {
                write!(
                    f,
                    "'{:02}:{:02}:{:02}.{:09}'",
                    t / 3_600_000_000_000,
                    t / 60_000_000_000 % 60,
                    t / 1_000_000_000 % 60,
                    t % 1_000_000_000,
                )?;
            }
            CqlValue::Timestamp(ts) => match ts.try_to_chrono_04_datetime_utc() {
                Ok(d) => write!(f, "{}", d.format("'%Y-%m-%d %H:%M:%S%.3f%z'"))?,
                Err(_) => f.write_str("<timestamp out of representable range>")?,
            },
            CqlValue::Timeuuid(t) => write!(f, "{}", t)?,
            CqlValue::Uuid(u) => write!(f, "{}", u)?,

            // Compound types
            CqlValue::Tuple(t) => {
                f.write_str("(")?;
                t.iter()
                    .map(|x| MaybeNullDisplayer(x.as_ref()))
                    .format(",")
                    .fmt(f)?;
                f.write_str(")")?;
            }
            CqlValue::List(v) => {
                f.write_str("[")?;
                v.iter().format(",").fmt(f)?;
                f.write_str("]")?;
            }
            CqlValue::Set(v) => {
                f.write_str("{")?;
                v.iter().format(",").fmt(f)?;
                f.write_str("}")?;
            }
            CqlValue::Map(m) => {
                f.write_str("{")?;
                m.iter()
                    .map(|(k, v)| PairDisplayer(k, v))
                    .format(",")
                    .fmt(f)?;
                f.write_str("}")?;
            }
            CqlValue::Vector(v) => {
                f.write_str("[")?;
                v.iter().format(",").fmt(f)?;
                f.write_str("]")?;
            }
            CqlValue::UserDefinedType {
                keyspace: _,
                name: _,
                fields,
            } => {
                f.write_str("{")?;
                fields
                    .iter()
                    .map(|(k, v)| PairDisplayer(k, MaybeNullDisplayer(v.as_ref())))
                    .format(",")
                    .fmt(f)?;
                f.write_str("}")?;
            }
        }
        Ok(())
    }
}

pub fn deser_cql_value(
    typ: &ColumnType,
    buf: &mut &[u8],
) -> StdResult<CqlValue, DeserializationError> {
    use crate::frame::response::result::ColumnType::*;
    use crate::frame::response::result::NativeType::*;

    if buf.is_empty() {
        match typ {
            Native(Ascii) | Native(Blob) | Native(Text) => {
                // can't be empty
            }
            _ => return Ok(CqlValue::Empty),
        }
    }
    // The `new_borrowed` version of FrameSlice is deficient in that it does not hold
    // a `Bytes` reference to the frame, only a slice.
    // This is not a problem here, fortunately, because none of CqlValue variants contain
    // any `Bytes` - only exclusively owned types - so we never call FrameSlice::to_bytes().
    let v = Some(FrameSlice::new_borrowed(buf));

    Ok(match typ {
        Native(Ascii) => {
            let s = String::deserialize(typ, v)?;
            CqlValue::Ascii(s)
        }
        Native(Boolean) => {
            let b = bool::deserialize(typ, v)?;
            CqlValue::Boolean(b)
        }
        Native(Blob) => {
            let b = Vec::<u8>::deserialize(typ, v)?;
            CqlValue::Blob(b)
        }
        Native(Date) => {
            let d = CqlDate::deserialize(typ, v)?;
            CqlValue::Date(d)
        }
        Native(Counter) => {
            let c = crate::value::Counter::deserialize(typ, v)?;
            CqlValue::Counter(c)
        }
        Native(Decimal) => {
            let d = CqlDecimal::deserialize(typ, v)?;
            CqlValue::Decimal(d)
        }
        Native(Double) => {
            let d = f64::deserialize(typ, v)?;
            CqlValue::Double(d)
        }
        Native(Float) => {
            let f = f32::deserialize(typ, v)?;
            CqlValue::Float(f)
        }
        Native(Int) => {
            let i = i32::deserialize(typ, v)?;
            CqlValue::Int(i)
        }
        Native(SmallInt) => {
            let si = i16::deserialize(typ, v)?;
            CqlValue::SmallInt(si)
        }
        Native(TinyInt) => {
            let ti = i8::deserialize(typ, v)?;
            CqlValue::TinyInt(ti)
        }
        Native(BigInt) => {
            let bi = i64::deserialize(typ, v)?;
            CqlValue::BigInt(bi)
        }
        Native(Text) => {
            let s = String::deserialize(typ, v)?;
            CqlValue::Text(s)
        }
        Native(Timestamp) => {
            let t = CqlTimestamp::deserialize(typ, v)?;
            CqlValue::Timestamp(t)
        }
        Native(Time) => {
            let t = CqlTime::deserialize(typ, v)?;
            CqlValue::Time(t)
        }
        Native(Timeuuid) => {
            let t = CqlTimeuuid::deserialize(typ, v)?;
            CqlValue::Timeuuid(t)
        }
        Native(Duration) => {
            let d = CqlDuration::deserialize(typ, v)?;
            CqlValue::Duration(d)
        }
        Native(Inet) => {
            let i = IpAddr::deserialize(typ, v)?;
            CqlValue::Inet(i)
        }
        Native(Uuid) => {
            let uuid = uuid::Uuid::deserialize(typ, v)?;
            CqlValue::Uuid(uuid)
        }
        Native(Varint) => {
            let vi = CqlVarint::deserialize(typ, v)?;
            CqlValue::Varint(vi)
        }
        Collection {
            typ: CollectionType::List(_type_name),
            ..
        } => {
            let l = Vec::<CqlValue>::deserialize(typ, v)?;
            CqlValue::List(l)
        }
        Collection {
            typ: CollectionType::Map(_key_type, _value_type),
            ..
        } => {
            let iter = MapIterator::<'_, '_, CqlValue, CqlValue>::deserialize(typ, v)?;
            let m: Vec<(CqlValue, CqlValue)> = iter.collect::<StdResult<_, _>>()?;
            CqlValue::Map(m)
        }
        Collection {
            typ: CollectionType::Set(_type_name),
            ..
        } => {
            let s = Vec::<CqlValue>::deserialize(typ, v)?;
            CqlValue::Set(s)
        }
        Vector { .. } => {
            return Err(mk_deser_err::<CqlValue>(
                typ,
                BuiltinDeserializationErrorKind::Unsupported,
            ))
        }
        UserDefinedType {
            definition: udt, ..
        } => {
            let iter = UdtIterator::deserialize(typ, v)?;
            let fields: Vec<(String, Option<CqlValue>)> = iter
                .map(|((col_name, col_type), res)| {
                    res.and_then(|v| {
                        let val = Option::<CqlValue>::deserialize(col_type, v.flatten())?;
                        Ok((col_name.clone().into_owned(), val))
                    })
                })
                .collect::<StdResult<_, _>>()?;

            CqlValue::UserDefinedType {
                keyspace: udt.keyspace.clone().into_owned(),
                name: udt.name.clone().into_owned(),
                fields,
            }
        }
        Tuple(type_names) => {
            let t = type_names
                .iter()
                .map(|typ| -> StdResult<_, DeserializationError> {
                    let raw = types::read_bytes_opt(buf).map_err(|e| {
                        mk_deser_err::<CqlValue>(
                            typ,
                            BuiltinDeserializationErrorKind::RawCqlBytesReadError(e),
                        )
                    })?;
                    raw.map(|v| CqlValue::deserialize(typ, Some(FrameSlice::new_borrowed(v))))
                        .transpose()
                })
                .collect::<StdResult<_, _>>()?;
            CqlValue::Tuple(t)
        }
    })
}

#[derive(Debug, Default, PartialEq)]
pub struct Row {
    pub columns: Vec<Option<CqlValue>>,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use super::*;

    #[test]
    fn timeuuid_msb_byte_order() {
        let uuid = CqlTimeuuid::from_str("00010203-0405-0607-0809-0a0b0c0d0e0f").unwrap();

        assert_eq!(0x0607040500010203, uuid.msb());
    }

    #[test]
    fn timeuuid_msb_clears_version_bits() {
        // UUID version nibble should be cleared
        let uuid = CqlTimeuuid::from_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();

        assert_eq!(0x0fffffffffffffff, uuid.msb());
    }

    #[test]
    fn timeuuid_lsb_byte_order() {
        let uuid = CqlTimeuuid::from_str("00010203-0405-0607-0809-0a0b0c0d0e0f").unwrap();

        assert_eq!(0x08090a0b0c0d0e0f, uuid.lsb());
    }

    #[test]
    fn timeuuid_lsb_modifies_no_bits() {
        let uuid = CqlTimeuuid::from_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();

        assert_eq!(0xffffffffffffffff, uuid.lsb());
    }

    #[test]
    fn timeuuid_nil() {
        let uuid = CqlTimeuuid::nil();

        assert_eq!(0x0000000000000000, uuid.msb());
        assert_eq!(0x0000000000000000, uuid.lsb());
    }

    #[test]
    fn test_cql_value_displayer() {
        assert_eq!(format!("{}", CqlValue::Boolean(true)), "true");
        assert_eq!(format!("{}", CqlValue::Int(123)), "123");
        assert_eq!(
            format!(
                "{}",
                // 123.456
                CqlValue::Decimal(CqlDecimal::from_signed_be_bytes_and_exponent(
                    vec![0x01, 0xE2, 0x40],
                    3
                ))
            ),
            "blobAsDecimal(0x0000000301e240)"
        );
        assert_eq!(format!("{}", CqlValue::Float(12.75)), "12.75");
        assert_eq!(
            format!("{}", CqlValue::Text("Ala ma kota".to_owned())),
            "'Ala ma kota'"
        );
        assert_eq!(
            format!("{}", CqlValue::Text("Foo's".to_owned())),
            "'Foo''s'"
        );

        // Time types are the most tricky
        assert_eq!(
            format!("{}", CqlValue::Date(CqlDate(40 + (1 << 31)))),
            "'1970-02-10'"
        );
        assert_eq!(
            format!(
                "{}",
                CqlValue::Duration(CqlDuration {
                    months: 1,
                    days: 2,
                    nanoseconds: 3,
                })
            ),
            "1mo2d3ns"
        );
        let t = chrono_04::NaiveTime::from_hms_nano_opt(6, 5, 4, 123)
            .unwrap()
            .signed_duration_since(chrono_04::NaiveTime::MIN);
        let t = t.num_nanoseconds().unwrap();
        assert_eq!(
            format!("{}", CqlValue::Time(CqlTime(t))),
            "'06:05:04.000000123'"
        );

        let t = chrono_04::NaiveDate::from_ymd_opt(2005, 4, 2)
            .unwrap()
            .and_time(chrono_04::NaiveTime::from_hms_opt(19, 37, 42).unwrap());
        assert_eq!(
            format!(
                "{}",
                CqlValue::Timestamp(CqlTimestamp(
                    t.signed_duration_since(chrono_04::NaiveDateTime::default())
                        .num_milliseconds()
                ))
            ),
            "'2005-04-02 19:37:42.000+0000'"
        );

        // Compound types
        let list_or_set = vec![CqlValue::Int(1), CqlValue::Int(3), CqlValue::Int(2)];
        assert_eq!(
            format!("{}", CqlValue::List(list_or_set.clone())),
            "[1,3,2]"
        );
        assert_eq!(format!("{}", CqlValue::Set(list_or_set.clone())), "{1,3,2}");

        let tuple: Vec<_> = list_or_set
            .into_iter()
            .map(Some)
            .chain(std::iter::once(None))
            .collect();
        assert_eq!(format!("{}", CqlValue::Tuple(tuple)), "(1,3,2,null)");

        let map = vec![
            (CqlValue::Text("foo".to_owned()), CqlValue::Int(123)),
            (CqlValue::Text("bar".to_owned()), CqlValue::Int(321)),
        ];
        assert_eq!(format!("{}", CqlValue::Map(map)), "{'foo':123,'bar':321}");

        let fields = vec![
            ("foo".to_owned(), Some(CqlValue::Int(123))),
            ("bar".to_owned(), Some(CqlValue::Int(321))),
        ];
        assert_eq!(
            format!(
                "{}",
                CqlValue::UserDefinedType {
                    keyspace: "ks".to_owned(),
                    name: "typ".to_owned(),
                    fields,
                }
            ),
            "{foo:123,bar:321}"
        );
    }
}
