use bytes::{Buf, Bytes};
use rand::Rng;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::num::Wrapping;
use std::ops::Bound::{Included, Unbounded};
use thiserror::Error;

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Node {
    // TODO: potentially a node may have multiple addresses, remember them?
    // but we need an Ord instance on Node
    pub addr: SocketAddr,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub struct Token {
    pub value: i64,
}

#[derive(Clone)]
pub struct Ring {
    // invariant: nonempty
    pub owners: BTreeMap<Token, Node>,
}

pub type Shard = u32;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct ShardInfo {
    shard: u16,
    nr_shards: u16,
    msb_ignore: u8,
}

impl std::str::FromStr for Token {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Token, std::num::ParseIntError> {
        Ok(Token { value: s.parse()? })
    }
}

pub fn murmur3_token(pk: Bytes) -> Token {
    Token {
        value: hash3_x64_128(&pk) as i64,
    }
}

impl Ring {
    pub fn owner(&self, t: Token) -> Node {
        if let Some((_, &n)) = self.owners.range((Included(t), Unbounded)).next() {
            return n;
        }
        *self.owners.iter().next().unwrap().1 // safe by invariant
    }
}

impl ShardInfo {
    pub fn new(shard: u16, nr_shards: u16, msb_ignore: u8) -> Self {
        assert!(nr_shards > 0);
        ShardInfo {
            shard,
            nr_shards,
            msb_ignore,
        }
    }

    pub fn shard_of(&self, token: Token) -> Shard {
        let mut biased_token = (token.value as u64).wrapping_add(1u64 << 63);
        biased_token <<= self.msb_ignore;
        (((biased_token as u128) * (self.nr_shards as u128)) >> 64) as Shard
    }

    /// If we connect to Scylla using Scylla's shard aware port, then Scylla assigns a shard to the
    /// connection based on the source port. This calculates the assigned shard.
    pub fn shard_of_source_port(&self, source_port: u16) -> Shard {
        (source_port % self.nr_shards) as Shard
    }

    /// Randomly choose a source port `p` such that `shard_of(t) == shard_of_source_port(p)`.
    pub fn draw_source_port_for_token(&self, t: Token) -> u16 {
        let shard = self.shard_of(t) as u16;
        assert!(shard < self.nr_shards);
        rand::thread_rng().gen_range(49152 + self.nr_shards - 1, 65535 - self.nr_shards + 1)
            / self.nr_shards
            * self.nr_shards
            + shard
    }
}

#[derive(Error, Debug)]
pub enum ShardingError {
    #[error("ShardInfo parameters missing")]
    MissingShardInfoParameter,
    #[error("ShardInfo parameters missing after unwraping")]
    MissingUnwrapedShardInfoParameter,
    #[error("ParseIntError encountered while getting ShardInfo")]
    ParseIntError(#[from] std::num::ParseIntError),
}

impl<'a> TryFrom<&'a HashMap<String, Vec<String>>> for ShardInfo {
    type Error = ShardingError;
    fn try_from(options: &'a HashMap<String, Vec<String>>) -> Result<Self, Self::Error> {
        let shard_entry = options.get("SCYLLA_SHARD");
        let nr_shards_entry = options.get("SCYLLA_NR_SHARDS");
        let msb_ignore_entry = options.get("SCYLLA_SHARDING_IGNORE_MSB");
        if shard_entry.is_none() || nr_shards_entry.is_none() || msb_ignore_entry.is_none() {
            return Err(ShardingError::MissingShardInfoParameter);
        }
        if shard_entry.unwrap().is_empty()
            || nr_shards_entry.unwrap().is_empty()
            || msb_ignore_entry.unwrap().is_empty()
        {
            return Err(ShardingError::MissingUnwrapedShardInfoParameter);
        }
        let shard = shard_entry.unwrap().first().unwrap().parse::<u16>()?;
        let nr_shards = nr_shards_entry.unwrap().first().unwrap().parse::<u16>()?;
        let msb_ignore = msb_ignore_entry.unwrap().first().unwrap().parse::<u8>()?;
        Ok(ShardInfo::new(shard, nr_shards, msb_ignore))
    }
}

#[test]
fn test_shard_of() {
    /* Test values taken from the gocql driver.  */
    let shard_info = ShardInfo::new(0, 4, 12);
    assert_eq!(
        shard_info.shard_of(Token {
            value: -9219783007514621794
        }),
        3
    );
    assert_eq!(
        shard_info.shard_of(Token {
            value: 9222582454147032830
        }),
        3
    );
}

// An implementation of MurmurHash3 ported from Scylla. Please note that this
// is not a "correct" implementation of MurmurHash3 - it replicates the same
// bugs made in the original Cassandra implementation in order to be compatible.
pub fn hash3_x64_128(mut data: &[u8]) -> i128 {
    let length = data.len();

    let c1: Wrapping<i64> = Wrapping(0x87c3_7b91_1142_53d5_u64 as i64);
    let c2: Wrapping<i64> = Wrapping(0x4cf5_ad43_2745_937f_u64 as i64);

    let mut h1 = Wrapping(0_i64);
    let mut h2 = Wrapping(0_i64);

    while data.len() >= 16 {
        let mut k1 = Wrapping(data.get_i64_le());
        let mut k2 = Wrapping(data.get_i64_le());

        k1 *= c1;
        k1 = rotl64(k1, 31);
        k1 *= c2;
        h1 ^= k1;

        h1 = rotl64(h1, 27);
        h1 += h2;
        h1 = h1 * Wrapping(5) + Wrapping(0x52dce729);

        k2 *= c2;
        k2 = rotl64(k2, 33);
        k2 *= c1;
        h2 ^= k2;

        h2 = rotl64(h2, 31);
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
        k2 = rotl64(k2, 33);
        k2 *= c1;
        h2 ^= k2;
    }

    if !data.is_empty() {
        for i in (0..std::cmp::min(8, data.len())).rev() {
            k1 ^= Wrapping(data[i] as i8 as i64) << (i * 8);
        }

        k1 *= c1;
        k1 = rotl64(k1, 31);
        k1 *= c2;
        h1 ^= k1;
    }

    h1 ^= Wrapping(length as i64);
    h2 ^= Wrapping(length as i64);

    h1 += h2;
    h2 += h1;

    h1 = fmix(h1);
    h2 = fmix(h2);

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
