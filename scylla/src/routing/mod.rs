use rand::Rng;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::num::NonZeroU16;
use thiserror::Error;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]

/// Token is a result of computing a hash of a primary key
///
/// It is basically an i64 with one caveat: i64::MIN is not
/// a valid token. It is used to represent infinity.
/// For this reason tokens are normalized - i64::MIN
/// is replaced with i64::MAX. See this fragment of
/// Scylla code for more information:
/// <https://github.com/scylladb/scylladb/blob/4be70bfc2bc7f133cab492b4aac7bab9c790a48c/dht/token.hh#L32>
///
/// This struct is a wrapper over i64 that performs this normalization
/// when initialized using `new()` method.
pub struct Token {
    value: i64,
}

impl Token {
    /// Creates a new token with given value, normalizing the value if necessary
    #[inline]
    pub fn new(value: i64) -> Self {
        Self {
            value: if value == i64::MIN { i64::MAX } else { value },
        }
    }

    /// Invalid Token - contains i64::MIN as value.
    ///
    /// This is (currently) only required by CDCPartitioner.
    /// See the following comment:
    /// https://github.com/scylladb/scylla-rust-driver/blob/049dc3546d24e45106fed0fdb985ec2511ab5192/scylla/src/transport/partitioner.rs#L312-L322
    pub(crate) const INVALID: Self = Token { value: i64::MIN };

    #[inline]
    pub fn value(&self) -> i64 {
        self.value
    }
}

pub type Shard = u32;
pub type ShardCount = NonZeroU16;

#[derive(PartialEq, Eq, Clone, Debug)]
pub(crate) struct ShardInfo {
    pub(crate) shard: u16,
    pub(crate) nr_shards: ShardCount,
    pub(crate) msb_ignore: u8,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Sharder {
    pub nr_shards: ShardCount,
    pub msb_ignore: u8,
}

impl std::str::FromStr for Token {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Token, std::num::ParseIntError> {
        Ok(Token { value: s.parse()? })
    }
}

impl ShardInfo {
    pub(crate) fn new(shard: u16, nr_shards: ShardCount, msb_ignore: u8) -> Self {
        ShardInfo {
            shard,
            nr_shards,
            msb_ignore,
        }
    }

    pub(crate) fn get_sharder(&self) -> Sharder {
        Sharder::new(self.nr_shards, self.msb_ignore)
    }
}

impl Sharder {
    pub fn new(nr_shards: ShardCount, msb_ignore: u8) -> Self {
        Sharder {
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
    /// Stops once all possible ports have been returned
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
}

#[derive(Clone, Error, Debug)]
pub enum ShardingError {
    #[error("ShardInfo parameters missing")]
    MissingShardInfoParameter,
    #[error("ShardInfo parameters missing after unwrapping")]
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

#[cfg(test)]
mod tests {
    use crate::test_utils::setup_tracing;

    use super::Token;
    use super::{ShardCount, Sharder};
    use std::collections::HashSet;

    #[test]
    fn test_shard_of() {
        setup_tracing();
        /* Test values taken from the gocql driver.  */
        let sharder = Sharder::new(ShardCount::new(4).unwrap(), 12);
        assert_eq!(
            sharder.shard_of(Token {
                value: -9219783007514621794
            }),
            3
        );
        assert_eq!(
            sharder.shard_of(Token {
                value: 9222582454147032830
            }),
            3
        );
    }

    #[test]
    fn test_iter_source_ports_for_shard() {
        setup_tracing();
        let nr_shards = 4;
        let max_port_num = 65535;
        let min_port_num = (49152 + nr_shards - 1) / nr_shards * nr_shards;

        let sharder = Sharder::new(ShardCount::new(nr_shards).unwrap(), 12);

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

            let port_iter = sharder.iter_source_ports_for_shard(shard.into());

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
