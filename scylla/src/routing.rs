use anyhow;
use std::collections::HashMap;
use std::convert::TryFrom;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Token {
    pub value: i64,
}

pub type Shard = u32;

#[derive(Debug)]
pub struct ShardInfo {
    nr_shards: u16,
    msb_ignore: u8,
}

impl ShardInfo {
    pub fn new(nr_shards: u16, msb_ignore: u8) -> Self {
        ShardInfo {
            nr_shards,
            msb_ignore,
        }
    }

    pub fn shard_of(&self, token: Token) -> Shard {
        let mut biased_token = (token.value as u64).wrapping_add(1u64 << 63);
        biased_token <<= self.msb_ignore;
        return (((biased_token as u128) * (self.nr_shards as u128)) >> 64) as Shard;
    }
}

impl<'a> TryFrom<&'a HashMap<String, Vec<String>>> for ShardInfo {
    type Error = anyhow::Error;
    fn try_from(options: &'a HashMap<String, Vec<String>>) -> Result<Self, Self::Error> {
        let nr_shards_entry = options.get("SCYLLA_NR_SHARDS");
        let msb_ignore_entry = options.get("SCYLLA_SHARDING_IGNORE_MSB");
        if nr_shards_entry.is_none() || msb_ignore_entry.is_none() {
            return Err(anyhow!("ShardInfo parameters missing"));
        }
        if nr_shards_entry.unwrap().is_empty() || msb_ignore_entry.unwrap().is_empty() {
            return Err(anyhow!("ShardInfo parameters missing"));
        }
        let nr_shards = nr_shards_entry.unwrap().first().unwrap().parse::<u16>()?;
        let msb_ignore = msb_ignore_entry.unwrap().first().unwrap().parse::<u8>()?;
        Ok(ShardInfo::new(nr_shards, msb_ignore))
    }
}

#[test]
fn test_shard_of() {
    /* Test values taken from the gocql driver.  */
    let shard_info = ShardInfo::new(4, 12);
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
