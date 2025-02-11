//! Partitioners are algorithms that can compute token for a given partition key,
//! ultimately allowing optimised routing of requests (such that a request is routed
//! to replicas, which are nodes and shards that really own the data the request concerns).
//! Currently, two partitioners are supported:
//! - Murmur3Partitioner
//!     - the default partitioner,
//!     - modified for compatibility with Cassandra's buggy implementation.
//! - CDCPartitioner
//!     - the partitioner employed when using CDC (_Change Data Capture_).

use bytes::Buf;
use scylla_cql::frame::types::RawValue;
use scylla_cql::serialize::row::SerializedValues;
use std::num::Wrapping;

use crate::{prepared_statement::TokenCalculationError, routing::Token};

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, PartialEq, Eq, Debug, Default)]
#[non_exhaustive]
pub enum PartitionerName {
    #[default]
    Murmur3,
    CDC,
}

impl PartitionerName {
    pub(crate) fn from_str(name: &str) -> Option<Self> {
        if name.ends_with("Murmur3Partitioner") {
            Some(PartitionerName::Murmur3)
        } else if name.ends_with("CDCPartitioner") {
            Some(PartitionerName::CDC)
        } else {
            None
        }
    }
}

impl Partitioner for PartitionerName {
    type Hasher = PartitionerHasherAny;

    fn build_hasher(&self) -> Self::Hasher {
        match self {
            PartitionerName::Murmur3 => {
                PartitionerHasherAny::Murmur3(Murmur3Partitioner.build_hasher())
            }
            PartitionerName::CDC => PartitionerHasherAny::CDC(CDCPartitioner.build_hasher()),
        }
    }
}

#[allow(clippy::upper_case_acronyms)]
pub(crate) enum PartitionerHasherAny {
    Murmur3(Murmur3PartitionerHasher),
    CDC(CDCPartitionerHasher),
}

impl PartitionerHasher for PartitionerHasherAny {
    fn write(&mut self, pk_part: &[u8]) {
        match self {
            PartitionerHasherAny::Murmur3(h) => h.write(pk_part),
            PartitionerHasherAny::CDC(h) => h.write(pk_part),
        }
    }

    fn finish(&self) -> Token {
        match self {
            PartitionerHasherAny::Murmur3(h) => h.finish(),
            PartitionerHasherAny::CDC(h) => h.finish(),
        }
    }
}

/// A trait for creating instances of `PartitionHasher`, which ultimately compute the token.
///
/// The Partitioners' design is based on std::hash design: `Partitioner`
/// corresponds to `HasherBuilder`, and `PartitionerHasher` to `Hasher`.
/// See their documentation for more details.
pub(crate) trait Partitioner {
    type Hasher: PartitionerHasher;

    fn build_hasher(&self) -> Self::Hasher;

    #[allow(unused)] // Currently, no public API uses this.
    fn hash_one(&self, data: &[u8]) -> Token {
        let mut hasher = self.build_hasher();
        hasher.write(data);
        hasher.finish()
    }
}

/// A trait for hashing a stream of serialized CQL values.
///
/// Instances of this trait are created by a `Partitioner` and are stateful.
/// At any point, one can call `finish()` and a `Token` will be computed
/// based on values that has been fed so far.
pub(crate) trait PartitionerHasher {
    fn write(&mut self, pk_part: &[u8]);
    fn finish(&self) -> Token;
}

pub(crate) struct Murmur3Partitioner;

impl Partitioner for Murmur3Partitioner {
    type Hasher = Murmur3PartitionerHasher;

    fn build_hasher(&self) -> Self::Hasher {
        Self::Hasher {
            total_len: 0,
            buf: Default::default(),
            h1: Wrapping(0),
            h2: Wrapping(0),
        }
    }
}

pub(crate) struct Murmur3PartitionerHasher {
    total_len: usize,
    buf: [u8; Self::BUF_CAPACITY],
    h1: Wrapping<i64>,
    h2: Wrapping<i64>,
}

impl Murmur3PartitionerHasher {
    const BUF_CAPACITY: usize = 16;

    const C1: Wrapping<i64> = Wrapping(0x87c3_7b91_1142_53d5_u64 as i64);
    const C2: Wrapping<i64> = Wrapping(0x4cf5_ad43_2745_937f_u64 as i64);

    fn hash_16_bytes(&mut self, mut k1: Wrapping<i64>, mut k2: Wrapping<i64>) {
        k1 *= Self::C1;
        k1 = Self::rotl64(k1, 31);
        k1 *= Self::C2;
        self.h1 ^= k1;

        self.h1 = Self::rotl64(self.h1, 27);
        self.h1 += self.h2;
        self.h1 = self.h1 * Wrapping(5) + Wrapping(0x52dce729);

        k2 *= Self::C2;
        k2 = Self::rotl64(k2, 33);
        k2 *= Self::C1;
        self.h2 ^= k2;

        self.h2 = Self::rotl64(self.h2, 31);
        self.h2 += self.h1;
        self.h2 = self.h2 * Wrapping(5) + Wrapping(0x38495ab5);
    }

    fn fetch_16_bytes_from_buf(buf: &mut &[u8]) -> (Wrapping<i64>, Wrapping<i64>) {
        let k1 = Wrapping(buf.get_i64_le());
        let k2 = Wrapping(buf.get_i64_le());
        (k1, k2)
    }

    #[inline]
    fn rotl64(v: Wrapping<i64>, n: u32) -> Wrapping<i64> {
        Wrapping((v.0 << n) | (v.0 as u64 >> (64 - n)) as i64)
    }

    #[inline]
    fn fmix(mut k: Wrapping<i64>) -> Wrapping<i64> {
        k ^= Wrapping((k.0 as u64 >> 33) as i64);
        k *= Wrapping(0xff51afd7ed558ccd_u64 as i64);
        k ^= Wrapping((k.0 as u64 >> 33) as i64);
        k *= Wrapping(0xc4ceb9fe1a85ec53_u64 as i64);
        k ^= Wrapping((k.0 as u64 >> 33) as i64);

        k
    }
}

// The implemented Murmur3 algorithm is roughly as follows:
// 1. while there are at least 16 bytes given:
//      consume 16 bytes by parsing them into i64s, then
//      include them in h1, h2, k1, k2;
// 2. do some magic with remaining n < 16 bytes,
//      include them in h1, h2, k1, k2;
// 3. compute the token based on h1, h2, k1, k2.
//
// Therefore, the buffer of capacity 16 is used. As soon as it gets full,
// point 1. is executed. Points 2. and 3. are exclusively done in `finish()`,
// so they don't mutate the state.
impl PartitionerHasher for Murmur3PartitionerHasher {
    fn write(&mut self, mut pk_part: &[u8]) {
        let mut buf_len = self.total_len % Self::BUF_CAPACITY;
        self.total_len += pk_part.len();

        // If the buffer is nonempty and can be filled completely, so that we can fetch two i64s from it,
        // fill it and hash its contents, then make it empty.
        if buf_len > 0 && Self::BUF_CAPACITY - buf_len <= pk_part.len() {
            // First phase: populate buffer until full, then consume two i64s.
            let to_write = Ord::min(Self::BUF_CAPACITY - buf_len, pk_part.len());
            self.buf[buf_len..buf_len + to_write].copy_from_slice(&pk_part[..to_write]);
            pk_part.advance(to_write);
            buf_len += to_write;

            debug_assert_eq!(buf_len, Self::BUF_CAPACITY);
            // consume 16 bytes from internal buf
            let mut buf_ptr = &self.buf[..];
            let (k1, k2) = Self::fetch_16_bytes_from_buf(&mut buf_ptr);
            debug_assert!(buf_ptr.is_empty());
            self.hash_16_bytes(k1, k2);
            buf_len = 0;
        }

        // If there were enough data, now we have an empty buffer. Further data, if enough, can be hence
        // hashed directly from the external buffer.
        if buf_len == 0 {
            // Second phase: fast path for big values.
            while pk_part.len() >= Self::BUF_CAPACITY {
                let (k1, k2) = Self::fetch_16_bytes_from_buf(&mut pk_part);
                self.hash_16_bytes(k1, k2);
            }
        }

        // Third phase: move remaining bytes to the buffer.
        debug_assert!(pk_part.len() < Self::BUF_CAPACITY - buf_len);
        let to_write = pk_part.len();
        self.buf[buf_len..buf_len + to_write].copy_from_slice(&pk_part[..to_write]);
        pk_part.advance(to_write);
        buf_len += to_write;
        debug_assert!(pk_part.is_empty());

        debug_assert!(buf_len < Self::BUF_CAPACITY);
    }

    fn finish(&self) -> Token {
        let mut h1 = self.h1;
        let mut h2 = self.h2;

        let mut k1 = Wrapping(0_i64);
        let mut k2 = Wrapping(0_i64);

        let buf_len = self.total_len % Self::BUF_CAPACITY;

        if buf_len > 8 {
            for i in (8..buf_len).rev() {
                k2 ^= Wrapping(self.buf[i] as i8 as i64) << ((i - 8) * 8);
            }

            k2 *= Self::C2;
            k2 = Self::rotl64(k2, 33);
            k2 *= Self::C1;
            h2 ^= k2;
        }

        if buf_len > 0 {
            for i in (0..std::cmp::min(8, buf_len)).rev() {
                k1 ^= Wrapping(self.buf[i] as i8 as i64) << (i * 8);
            }

            k1 *= Self::C1;
            k1 = Self::rotl64(k1, 31);
            k1 *= Self::C2;
            h1 ^= k1;
        }

        h1 ^= Wrapping(self.total_len as i64);
        h2 ^= Wrapping(self.total_len as i64);

        h1 += h2;
        h2 += h1;

        h1 = Self::fmix(h1);
        h2 = Self::fmix(h2);

        h1 += h2;
        h2 += h1;

        Token::new((((h2.0 as i128) << 64) | h1.0 as i128) as i64)
    }
}

enum CDCPartitionerHasherState {
    Feeding {
        len: usize,
        buf: [u8; CDCPartitionerHasher::BUF_CAPACITY],
    },
    Computed(Token),
}

pub(crate) struct CDCPartitioner;

pub(crate) struct CDCPartitionerHasher {
    state: CDCPartitionerHasherState,
}

impl Partitioner for CDCPartitioner {
    type Hasher = CDCPartitionerHasher;

    fn build_hasher(&self) -> Self::Hasher {
        Self::Hasher {
            state: CDCPartitionerHasherState::Feeding {
                len: 0,
                buf: Default::default(),
            },
        }
    }
}

impl CDCPartitionerHasher {
    const BUF_CAPACITY: usize = 8;
}

impl PartitionerHasher for CDCPartitionerHasher {
    fn write(&mut self, pk_part: &[u8]) {
        match &mut self.state {
            CDCPartitionerHasherState::Feeding { len, buf } => {
                // We feed the buffer until it's full.
                let copied_len = Ord::min(pk_part.len(), Self::BUF_CAPACITY - *len);
                buf[*len..*len + copied_len].copy_from_slice(&pk_part[..copied_len]);
                *len += copied_len;

                // If the buffer is full, we can compute and fix the token.
                if *len == Self::BUF_CAPACITY {
                    let token = Token::new((&mut &buf[..]).get_i64());
                    self.state = CDCPartitionerHasherState::Computed(token);
                }
            }
            CDCPartitionerHasherState::Computed(_) => (),
        }
    }

    fn finish(&self) -> Token {
        match self.state {
            // Looking at Scylla code it seems that here we actually want token with this value.
            // If the value is too short Scylla returns `dht::minimum_token()`:
            // https://github.com/scylladb/scylladb/blob/4be70bfc2bc7f133cab492b4aac7bab9c790a48c/cdc/cdc_partitioner.cc#L32
            // When you call `long_token` on `minimum_token` it will actually return `i64::MIN`:
            // https://github.com/scylladb/scylladb/blob/0a7854ea4de04f20b71326ba5940b5fac6f7241a/dht/token.cc#L21-L35
            CDCPartitionerHasherState::Feeding { .. } => Token::INVALID,
            CDCPartitionerHasherState::Computed(token) => token,
        }
    }
}

/// Calculates the token for given partitioner and serialized partition key.
///
/// The ordinary way to calculate token is based on a PreparedStatement
/// and values for that statement. However, if a user knows:
/// - the order of the columns in the partition key,
/// - the values of the columns of the partition key,
/// - the partitioner of the table that the statement operates on,
///
/// then having a `PreparedStatement` is not necessary and the token can
/// be calculated based on that information.
///
/// NOTE: the provided values must completely constitute partition key
/// and be in the order defined in CREATE TABLE statement.
pub fn calculate_token_for_partition_key(
    serialized_partition_key_values: &SerializedValues,
    partitioner: &PartitionerName,
) -> Result<Token, TokenCalculationError> {
    let mut partitioner_hasher = partitioner.build_hasher();

    if serialized_partition_key_values.element_count() == 1 {
        let val = serialized_partition_key_values.iter().next().unwrap();
        if let RawValue::Value(val) = val {
            partitioner_hasher.write(val);
        }
    } else {
        for val in serialized_partition_key_values
            .iter()
            .filter_map(|rv| rv.as_value())
        {
            let val_len_u16: u16 = val
                .len()
                .try_into()
                .map_err(|_| TokenCalculationError::ValueTooLong(val.len()))?;
            partitioner_hasher.write(&val_len_u16.to_be_bytes());
            partitioner_hasher.write(val);
            partitioner_hasher.write(&[0u8]);
        }
    }

    Ok(partitioner_hasher.finish())
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use rand_pcg::Pcg32;

    use crate::test_utils::setup_tracing;

    use super::{CDCPartitioner, Murmur3Partitioner, Partitioner, PartitionerHasher};

    fn assert_correct_murmur3_hash(pk: &'static str, expected_hash: i64) {
        let hash = Murmur3Partitioner.hash_one(pk.as_bytes()).value();
        assert_eq!(hash, expected_hash);
    }

    #[test]
    fn test_murmur3_partitioner() {
        setup_tracing();
        for s in [
            ("test", -6017608668500074083),
            ("xd", 4507812186440344727),
            ("primary_key", -1632642444691073360),
            ("kremówki", 4354931215268080151),
        ] {
            assert_correct_murmur3_hash(s.0, s.1);
        }
    }

    fn assert_correct_cdc_hash(pk: &'static str, expected_hash: i64) {
        let hash = CDCPartitioner.hash_one(pk.as_bytes()).value();
        assert_eq!(hash, expected_hash);
    }

    #[test]
    fn partitioners_output_same_result_no_matter_how_input_is_partitioned() {
        setup_tracing();
        let inputs: &[&[u8]] = &[
            b"",
            b"0",
            "Ala ma kota, a kota ma Ala.".as_bytes(),
            "Zażółć gęślą jaźń. Wsiadł rycerz Szaławiła na bułanego konia. Litwo, ojczyzno moja, ...".as_bytes(),
        ];

        let seed = 0x2137;
        let mut randgen = Pcg32::new(seed, 0);

        // Splits the given data 2^n times and feeds partitioner with the chunks got.
        fn split_and_feed(
            randgen: &mut impl Rng,
            partitioner: &mut impl PartitionerHasher,
            data: &[u8],
            n: usize,
        ) {
            if n == 0 {
                partitioner.write(data);
            } else {
                let pivot = if !data.is_empty() {
                    randgen.gen_range(0..data.len())
                } else {
                    0
                };
                let (data1, data2) = data.split_at(pivot);
                for data in [data1, data2] {
                    split_and_feed(randgen, partitioner, data, n - 1);
                }
            }
        }

        fn check_for_partitioner<P: Partitioner>(
            partitioner: P,
            randgen: &mut impl Rng,
            input: &[u8],
        ) {
            let result_single_batch = partitioner.hash_one(input);

            let results_chunks = (0..1000).map(|_| {
                let mut partitioner_hasher = partitioner.build_hasher();
                split_and_feed(randgen, &mut partitioner_hasher, input, 2);
                partitioner_hasher.finish()
            });

            for result_chunk in results_chunks {
                assert_eq!(result_single_batch, result_chunk)
            }
        }

        for input in inputs {
            check_for_partitioner(Murmur3Partitioner, &mut randgen, input);
            check_for_partitioner(CDCPartitioner, &mut randgen, input);
        }
    }

    #[test]
    fn test_cdc_partitioner() {
        setup_tracing();
        for s in [
            ("test", -9223372036854775808),
            ("xd", -9223372036854775808),
            ("primary_key", 8102654598100187487),
            ("kremówki", 7742362231512463211),
        ] {
            assert_correct_cdc_hash(s.0, s.1);
        }
    }
}
