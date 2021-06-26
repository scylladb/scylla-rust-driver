use bytes::{Buf, Bytes};
use rand::Rng;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::num::{NonZeroU16, Wrapping};
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

pub type Shard = u32;
pub type ShardCount = NonZeroU16;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct ShardInfo {
    pub shard: u16,
    pub nr_shards: ShardCount,
    pub msb_ignore: u8,
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

impl ShardInfo {
    pub fn new(shard: u16, nr_shards: ShardCount, msb_ignore: u8) -> Self {
        ShardInfo {
            shard,
            nr_shards,
            msb_ignore,
        }
    }

    pub fn shard_of(&self, token: Token) -> Shard {
        let mut biased_token = (token.value as u64).wrapping_add(1u64 << 63);
        biased_token <<= self.msb_ignore;
        (((biased_token as u128) * (self.nr_shards.get() as u128)) >> 64) as Shard
    }

    /// If we connect to Scylla using Scylla's shard aware port, then Scylla assigns a shard to the
    /// connection based on the source port. This calculates the assigned shard.
    pub fn shard_of_source_port(&self, source_port: u16) -> Shard {
        (source_port % self.nr_shards.get()) as Shard
    }

    /// Randomly choose a source port `p` such that `shard == shard_of_source_port(p)`.
    pub fn draw_source_port_for_shard(&self, shard: Shard) -> u16 {
        assert!(shard < self.nr_shards.get() as u32);
        rand::thread_rng()
            .gen_range((49152 + self.nr_shards.get() - 1)..(65535 - self.nr_shards.get() + 1))
            / self.nr_shards.get()
            * self.nr_shards.get()
            + shard as u16
    }

    /// Returns iterator over source ports `p` such that `shard == shard_of_source_port(p)`.
    /// Starts at a random port and goes forward by `nr_shards`. After reaching maximum wraps back around.
    /// Stops once all possibile ports have been returned
    pub fn iter_source_ports_for_shard(&self, shard: Shard) -> impl Iterator<Item = u16> {
        assert!(shard < self.nr_shards.get() as u32);

        // Randomly choose a port to start at
        let starting_port = self.draw_source_port_for_shard(shard);

        // Choose smallest available port number to begin at after wrapping
        // apply the formula from draw_source_port_for_shard for lowest possible gen_range result
        let first_valid_port = (49152 + self.nr_shards.get() - 1) / self.nr_shards.get()
            * self.nr_shards.get()
            + shard as u16;

        let before_wrap = (starting_port..=65535).step_by(self.nr_shards.get().into());
        let after_wrap = (first_valid_port..starting_port).step_by(self.nr_shards.get().into());

        before_wrap.chain(after_wrap)
    }

    pub fn get_nr_shards(&self) -> ShardCount {
        self.nr_shards
    }
}

#[derive(Error, Debug)]
pub enum ShardingError {
    #[error("ShardInfo parameters missing")]
    MissingShardInfoParameter,
    #[error("ShardInfo parameters missing after unwraping")]
    MissingUnwrapedShardInfoParameter,
    #[error("ShardInfo contains an invalid number of shards (0)")]
    ZeroShards,
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
        let nr_shards = ShardCount::new(nr_shards).ok_or(ShardingError::ZeroShards)?;
        let msb_ignore = msb_ignore_entry.unwrap().first().unwrap().parse::<u8>()?;
        Ok(ShardInfo::new(shard, nr_shards, msb_ignore))
    }
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

#[cfg(test)]
mod tests {
    use super::Token;
    use super::{ShardCount, ShardInfo};
    use std::collections::HashSet;

    #[test]
    fn test_shard_of() {
        /* Test values taken from the gocql driver.  */
        let shard_info = ShardInfo::new(0, ShardCount::new(4).unwrap(), 12);
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

    #[test]
    fn test_iter_source_ports_for_shard() {
        let nr_shards = 4;
        let max_port_num = 65535;
        let min_port_num = (49152 + nr_shards - 1) / nr_shards * nr_shards;

        let shard_info = ShardInfo::new(0, ShardCount::new(nr_shards).unwrap(), 12);

        // Test for each shard
        for shard in 0..nr_shards {
            // Find lowest port for this shard
            let mut lowest_port = min_port_num;
            while lowest_port % nr_shards != shard {
                lowest_port += 1;
            }

            // Find total number of ports the iterator should return
            let possible_ports_number: usize =
                ((max_port_num - lowest_port) / nr_shards + 1).into();

            let port_iter = shard_info.iter_source_ports_for_shard(shard.into());

            let mut returned_ports: HashSet<u16> = HashSet::new();
            for port in port_iter {
                assert!(!returned_ports.contains(&port)); // No port occurs two times
                assert!(port % nr_shards == shard); // Each port maps to this shard

                returned_ports.insert(port);
            }

            // Numbers of ports returned matches the expected value
            assert_eq!(returned_ports.len(), possible_ports_number);
        }
    }
}
