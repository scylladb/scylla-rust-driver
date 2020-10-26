use anyhow;
use anyhow::Result;
use bytes::Bytes;
use fasthash::murmur3;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::ops::Bound::{Included, Unbounded};

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

#[derive(Debug)]
pub struct ShardInfo {
    shard: u16,
    nr_shards: u16,
    msb_ignore: u8,
}

impl Token {
    pub fn from_str(s: &str) -> Result<Token> {
        Ok(Token { value: s.parse()? })
    }
}

pub fn murmur3_token(pk: Bytes) -> Token {
    Token {
        value: murmur3::hash128(pk) as i64,
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
        ShardInfo {
            shard,
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
        let shard_entry = options.get("SCYLLA_SHARD");
        let nr_shards_entry = options.get("SCYLLA_NR_SHARDS");
        let msb_ignore_entry = options.get("SCYLLA_SHARDING_IGNORE_MSB");
        if shard_entry.is_none() || nr_shards_entry.is_none() || msb_ignore_entry.is_none() {
            return Err(anyhow!("ShardInfo parameters missing"));
        }
        if shard_entry.unwrap().is_empty()
            || nr_shards_entry.unwrap().is_empty()
            || msb_ignore_entry.unwrap().is_empty()
        {
            return Err(anyhow!("ShardInfo parameters missing"));
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
