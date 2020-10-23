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
        let nr_shards = self.nr_shards as u64;
        let z = ((token.value.wrapping_add(i64::MIN)) as u64) << self.msb_ignore;
        let lo = z & 0xffffffff;
        let hi = (z >> 32) & 0xffffffff;
        let mul1 = lo * nr_shards;
        let mul2 = hi * nr_shards;
        let sum = (mul1 >> 32) + mul2;
        (sum >> 32) as Shard
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
