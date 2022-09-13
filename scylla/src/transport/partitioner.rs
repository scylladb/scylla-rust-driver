use bytes::{Buf, Bytes};
use std::num::Wrapping;

use crate::routing::Token;

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum PartitionerName {
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

    pub(crate) fn hash(&self, pk: Bytes) -> Token {
        match self {
            PartitionerName::Murmur3 => Murmur3Partitioner::hash(pk),
            PartitionerName::CDC => CDCPartitioner::hash(pk),
        }
    }
}

impl Default for PartitionerName {
    fn default() -> Self {
        PartitionerName::Murmur3
    }
}

pub trait Partitioner {
    fn hash(pk: Bytes) -> Token;
}

pub struct Murmur3Partitioner;
pub struct CDCPartitioner;

impl Murmur3Partitioner {
    // An implementation of MurmurHash3 ported from Scylla. Please note that this
    // is not a "correct" implementation of MurmurHash3 - it replicates the same
    // bugs made in the original Cassandra implementation in order to be compatible.
    fn hash3_x64_128(mut data: &[u8]) -> i128 {
        let length = data.len();

        let c1: Wrapping<i64> = Wrapping(0x87c3_7b91_1142_53d5_u64 as i64);
        let c2: Wrapping<i64> = Wrapping(0x4cf5_ad43_2745_937f_u64 as i64);

        let mut h1 = Wrapping(0_i64);
        let mut h2 = Wrapping(0_i64);

        while data.len() >= 16 {
            let mut k1 = Wrapping(data.get_i64_le());
            let mut k2 = Wrapping(data.get_i64_le());

            k1 *= c1;
            k1 = Self::rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = Self::rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * Wrapping(5) + Wrapping(0x52dce729);

            k2 *= c2;
            k2 = Self::rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;

            h2 = Self::rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * Wrapping(5) + Wrapping(0x38495ab5);
        }

        let mut k1 = Wrapping(0_i64);
        let mut k2 = Wrapping(0_i64);

        debug_assert!(data.len() < 16);

        if data.len() > 8 {
            for i in (8..data.len()).rev() {
                k2 ^= Wrapping(data[i] as i8 as i64) << ((i - 8) * 8);
            }

            k2 *= c2;
            k2 = Self::rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;
        }

        if !data.is_empty() {
            for i in (0..std::cmp::min(8, data.len())).rev() {
                k1 ^= Wrapping(data[i] as i8 as i64) << (i * 8);
            }

            k1 *= c1;
            k1 = Self::rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;
        }

        h1 ^= Wrapping(length as i64);
        h2 ^= Wrapping(length as i64);

        h1 += h2;
        h2 += h1;

        h1 = Self::fmix(h1);
        h2 = Self::fmix(h2);

        h1 += h2;
        h2 += h1;

        ((h2.0 as i128) << 64) | h1.0 as i128
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

impl Partitioner for Murmur3Partitioner {
    fn hash(pk: Bytes) -> Token {
        Token {
            value: Self::hash3_x64_128(&pk) as i64,
        }
    }
}

impl Partitioner for CDCPartitioner {
    fn hash(mut pk: Bytes) -> Token {
        let value = if pk.len() < 8 { i64::MIN } else { pk.get_i64() };
        Token { value }
    }
}

#[cfg(test)]
mod tests {
    use super::{CDCPartitioner, Murmur3Partitioner, Partitioner};
    use bytes::Bytes;

    fn assert_correct_murmur3_hash(pk: &'static str, expected_hash: i64) {
        let pk = Bytes::from(pk);
        let hash = Murmur3Partitioner::hash(pk).value;
        assert_eq!(hash, expected_hash);
    }

    #[test]
    fn test_murmur3_partitioner() {
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
        let pk = Bytes::from(pk);
        let hash = CDCPartitioner::hash(pk).value;
        assert_eq!(hash, expected_hash);
    }

    #[test]
    fn test_cdc_partitioner() {
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
