use std::collections::HashMap;
use std::num::NonZeroU16;

use rand::Rng as _;
use thiserror::Error;

use super::Token;

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

const SHARD_ENTRY: &str = "SCYLLA_SHARD";
const NR_SHARDS_ENTRY: &str = "SCYLLA_NR_SHARDS";
const MSB_IGNORE_ENTRY: &str = "SCYLLA_SHARDING_IGNORE_MSB";

impl<'a> TryFrom<&'a HashMap<String, Vec<String>>> for ShardInfo {
    type Error = ShardingError;
    fn try_from(options: &'a HashMap<String, Vec<String>>) -> Result<Self, Self::Error> {
        let shard_entry = options.get(SHARD_ENTRY);
        let nr_shards_entry = options.get(NR_SHARDS_ENTRY);
        let msb_ignore_entry = options.get(MSB_IGNORE_ENTRY);

        // Unwrap entries.
        let (Some(shard_entry), Some(nr_shards_entry), Some(msb_ignore_entry)) =
            (shard_entry, nr_shards_entry, msb_ignore_entry)
        else {
            return Err(ShardingError::MissingShardInfoParameter);
        };

        // Further unwrap entries (they should be the first entries of their corresponding Vecs).
        let (Some(shard_entry), Some(nr_shards_entry), Some(msb_ignore_entry)) = (
            shard_entry.first(),
            nr_shards_entry.first(),
            msb_ignore_entry.first(),
        ) else {
            return Err(ShardingError::MissingUnwrapedShardInfoParameter);
        };

        let shard = shard_entry.parse::<u16>()?;
        let nr_shards = nr_shards_entry.parse::<u16>()?;
        let nr_shards = ShardCount::new(nr_shards).ok_or(ShardingError::ZeroShards)?;
        let msb_ignore = msb_ignore_entry.parse::<u8>()?;
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
