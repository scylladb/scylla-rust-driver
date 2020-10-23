#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct Token {
    pub value: i64,
}

pub type Shard = u32;

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
