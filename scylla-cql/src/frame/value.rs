use thiserror::Error;
use uuid::Uuid;

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
#[derive(Clone, Copy, Default)]
pub enum MaybeUnset<V> {
    #[default]
    Unset,
    Set(V),
}

/// Represents timeuuid (uuid V1) value
///
/// This type has custom comparison logic which follows Scylla/Cassandra semantics.
/// For details, see [`Ord` implementation](#impl-Ord-for-CqlTimeuuid).
#[derive(Debug, Clone, Copy, Eq)]
pub struct CqlTimeuuid(Uuid);

/// [`Uuid`] delegate methods
impl CqlTimeuuid {
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
        ((bytes[6] & 0x0F) as u64) << 56
            | (bytes[7] as u64) << 48
            | (bytes[4] as u64) << 40
            | (bytes[5] as u64) << 32
            | (bytes[0] as u64) << 24
            | (bytes[1] as u64) << 16
            | (bytes[2] as u64) << 8
            | (bytes[3] as u64)
    }

    fn lsb(&self) -> u64 {
        let bytes = self.0.as_bytes();
        (bytes[8] as u64) << 56
            | (bytes[9] as u64) << 48
            | (bytes[10] as u64) << 40
            | (bytes[11] as u64) << 32
            | (bytes[12] as u64) << 24
            | (bytes[13] as u64) << 16
            | (bytes[14] as u64) << 8
            | (bytes[15] as u64)
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
/// The library support (e.g. conversion from [`CqlValue`](super::response::result::CqlValue)) for these types is
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
/// # use scylla_cql::frame::value::CqlVarint;
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
/// # use scylla_cql::frame::value::CqlVarintBorrowed;
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
/// The library support (e.g. conversion from [`CqlValue`](super::response::result::CqlValue)) for the type is
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
impl From<chrono_04::DateTime<chrono_04::Utc>> for CqlTimestamp {
    fn from(value: chrono_04::DateTime<chrono_04::Utc>) -> Self {
        Self(value.timestamp_millis())
    }
}

#[cfg(feature = "chrono-04")]
impl TryInto<chrono_04::DateTime<chrono_04::Utc>> for CqlTimestamp {
    type Error = ValueOverflow;

    fn try_into(self) -> Result<chrono_04::DateTime<chrono_04::Utc>, Self::Error> {
        use chrono_04::TimeZone;
        match chrono_04::Utc.timestamp_millis_opt(self.0) {
            chrono_04::LocalResult::Single(datetime) => Ok(datetime),
            _ => Err(ValueOverflow),
        }
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
