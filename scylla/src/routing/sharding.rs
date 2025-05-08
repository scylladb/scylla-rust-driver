use std::collections::HashMap;
use std::num::NonZeroU16;
use std::ops::RangeInclusive;

use rand::Rng as _;
use thiserror::Error;

use super::Token;

/// A range of ports that can be used for shard-aware connections.
///
/// The range is inclusive and has to be a sub-range of [1024, 65535].
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct ShardAwarePortRange(RangeInclusive<u16>);

impl ShardAwarePortRange {
    /// The default shard-aware local port range - [49152, 65535].
    pub const EPHEMERAL_PORT_RANGE: Self = Self(49152..=65535);

    /// Creates a new `ShardAwarePortRange` with the given range.
    ///
    /// The error is returned in two cases:
    /// 1. Provided range is empty (`end` < `start`).
    /// 2. Provided range starts at a port lower than 1024. Ports 0-1023 are reserved and
    ///    should not be used by application.
    #[inline]
    pub fn new(range: impl Into<RangeInclusive<u16>>) -> Result<Self, InvalidShardAwarePortRange> {
        let range = range.into();
        if range.is_empty() || range.start() < &1024 {
            return Err(InvalidShardAwarePortRange);
        }
        Ok(Self(range))
    }
}

impl Default for ShardAwarePortRange {
    #[inline]
    fn default() -> Self {
        Self::EPHEMERAL_PORT_RANGE
    }
}

/// An error returned by [`ShardAwarePortRange::new()`].
#[derive(Debug, Error)]
#[error("Invalid shard-aware local port range")]
pub struct InvalidShardAwarePortRange;

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
    #[inline]
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
    #[inline]
    pub fn new(nr_shards: ShardCount, msb_ignore: u8) -> Self {
        Sharder {
            nr_shards,
            msb_ignore,
        }
    }

    #[inline]
    pub fn shard_of(&self, token: Token) -> Shard {
        let mut biased_token = (token.value as u64).wrapping_add(1u64 << 63);
        biased_token <<= self.msb_ignore;
        (((biased_token as u128) * (self.nr_shards.get() as u128)) >> 64) as Shard
    }

    /// If we connect to Scylla using Scylla's shard aware port, then Scylla assigns a shard to the
    /// connection based on the source port. This calculates the assigned shard.
    #[inline]
    pub fn shard_of_source_port(&self, source_port: u16) -> Shard {
        (source_port % self.nr_shards.get()) as Shard
    }

    /// Randomly choose a source port `p` such that `shard == shard_of_source_port(p)`.
    ///
    /// The port is chosen from ephemeral port range [49152, 65535].
    #[inline]
    pub fn draw_source_port_for_shard(&self, shard: Shard) -> u16 {
        self.draw_source_port_for_shard_from_range(
            shard,
            &ShardAwarePortRange::EPHEMERAL_PORT_RANGE,
        )
    }

    /// Randomly choose a source port `p` such that `shard == shard_of_source_port(p)`.
    ///
    /// The port is chosen from the provided port range.
    pub(crate) fn draw_source_port_for_shard_from_range(
        &self,
        shard: Shard,
        port_range: &ShardAwarePortRange,
    ) -> u16 {
        assert!(shard < self.nr_shards.get() as u32);
        let (range_start, range_end) = (port_range.0.start(), port_range.0.end());
        rand::rng().random_range(
            (range_start + self.nr_shards.get() - 1)..(range_end - self.nr_shards.get() + 1),
        ) / self.nr_shards.get()
            * self.nr_shards.get()
            + shard as u16
    }

    /// Returns iterator over source ports `p` such that `shard == shard_of_source_port(p)`.
    /// Starts at a random port and goes forward by `nr_shards`. After reaching maximum wraps back around.
    /// Stops once all possible ports have been returned
    ///
    /// The ports are chosen from ephemeral port range [49152, 65535].
    #[inline]
    pub fn iter_source_ports_for_shard(&self, shard: Shard) -> impl Iterator<Item = u16> {
        self.iter_source_ports_for_shard_from_range(
            shard,
            &ShardAwarePortRange::EPHEMERAL_PORT_RANGE,
        )
    }

    /// Returns iterator over source ports `p` such that `shard == shard_of_source_port(p)`.
    /// Starts at a random port and goes forward by `nr_shards`. After reaching maximum wraps back around.
    /// Stops once all possible ports have been returned
    ///
    /// The ports are chosen from the provided port range.
    pub(crate) fn iter_source_ports_for_shard_from_range(
        &self,
        shard: Shard,
        port_range: &ShardAwarePortRange,
    ) -> impl Iterator<Item = u16> {
        assert!(shard < self.nr_shards.get() as u32);

        let (range_start, range_end) = (port_range.0.start(), port_range.0.end());

        // Randomly choose a port to start at
        let starting_port = self.draw_source_port_for_shard_from_range(shard, port_range);

        // Choose smallest available port number to begin at after wrapping
        // apply the formula from draw_source_port_for_shard for lowest possible gen_range result
        let first_valid_port =
            range_start.div_ceil(self.nr_shards.get()) * self.nr_shards.get() + shard as u16;

        let before_wrap = (starting_port..=*range_end).step_by(self.nr_shards.get().into());
        let after_wrap = (first_valid_port..starting_port).step_by(self.nr_shards.get().into());

        before_wrap.chain(after_wrap)
    }
}

#[derive(Clone, Error, Debug)]
pub(crate) enum ShardingError {
    /// This indicates that we are most likely connected to a Cassandra cluster.
    /// Unless, there is some serious bug in Scylla.
    #[error("Server did not provide any sharding information")]
    NoShardInfo,

    /// A bug in scylla. Some of the parameters are present, while others are missing.
    #[error("Missing some sharding info parameters")]
    MissingSomeShardInfoParameters,

    /// A bug in Scylla. All parameters are present, but some do not contain any values.
    #[error("Missing some sharding info parameter values")]
    MissingShardInfoParameterValues,

    /// A bug in Scylla. Number of shards is equal to zero.
    #[error("Sharding info contains an invalid number of shards (0)")]
    ZeroShards,

    /// A bug in Scylla. Failed to parse string to number.
    #[error("Failed to parse a sharding info parameter's value: {0}")]
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
        let (shard_entry, nr_shards_entry, msb_ignore_entry) =
            match (shard_entry, nr_shards_entry, msb_ignore_entry) {
                (Some(shard_entry), Some(nr_shards_entry), Some(msb_ignore_entry)) => {
                    (shard_entry, nr_shards_entry, msb_ignore_entry)
                }
                // All parameters are missing - most likely a Cassandra cluster.
                (None, None, None) => return Err(ShardingError::NoShardInfo),
                // At least one of the parameters is present, but some are missing. A bug in Scylla.
                _ => return Err(ShardingError::MissingSomeShardInfoParameters),
            };

        // Further unwrap entries (they should be the first entries of their corresponding Vecs).
        let (Some(shard_entry), Some(nr_shards_entry), Some(msb_ignore_entry)) = (
            shard_entry.first(),
            nr_shards_entry.first(),
            msb_ignore_entry.first(),
        ) else {
            return Err(ShardingError::MissingShardInfoParameterValues);
        };

        let shard = shard_entry.parse::<u16>()?;
        let nr_shards = nr_shards_entry.parse::<u16>()?;
        let nr_shards = ShardCount::new(nr_shards).ok_or(ShardingError::ZeroShards)?;
        let msb_ignore = msb_ignore_entry.parse::<u8>()?;
        Ok(ShardInfo::new(shard, nr_shards, msb_ignore))
    }
}

#[cfg(test)]
impl ShardInfo {
    pub(crate) fn add_to_options(&self, options: &mut HashMap<String, Vec<String>>) {
        for (k, v) in [
            (SHARD_ENTRY, &self.shard as &dyn std::fmt::Display),
            (NR_SHARDS_ENTRY, &self.nr_shards),
            (MSB_IGNORE_ENTRY, &self.msb_ignore),
        ] {
            options.insert(k.to_owned(), vec![v.to_string()]);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::routing::{Shard, ShardAwarePortRange};
    use crate::test_utils::setup_tracing;

    use super::Token;
    use super::{ShardCount, Sharder};
    use std::collections::HashSet;

    #[test]
    fn test_shard_aware_port_range_constructor() {
        setup_tracing();

        // Test valid range
        let range = ShardAwarePortRange::new(49152..=65535).unwrap();
        assert_eq!(range, ShardAwarePortRange::EPHEMERAL_PORT_RANGE);

        // Test invalid range (empty)
        #[allow(clippy::reversed_empty_ranges)]
        {
            assert!(ShardAwarePortRange::new(49152..=49151).is_err());
        }
        // Test invalid range (too low)
        assert!(ShardAwarePortRange::new(0..=65535).is_err());
    }

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

        fn test_helper<F, I>(nr_shards: u16, port_range: ShardAwarePortRange, get_iter: F)
        where
            F: Fn(&Sharder, Shard) -> I,
            I: Iterator<Item = u16>,
        {
            let max_port_num = port_range.0.end();
            let min_port_num = port_range.0.start().div_ceil(nr_shards) * nr_shards;

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

                let port_iter = get_iter(&sharder, shard.into());

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

        // Test of public method (with default range)
        {
            test_helper(
                4,
                ShardAwarePortRange::EPHEMERAL_PORT_RANGE,
                |sharder, shard| sharder.iter_source_ports_for_shard(shard),
            );
        }

        // Test of private method with some custom port range.
        {
            let port_range = ShardAwarePortRange::new(21371..=42424).unwrap();
            test_helper(4, port_range.clone(), |sharder, shard| {
                sharder.iter_source_ports_for_shard_from_range(shard, &port_range)
            });
        }
    }
}
