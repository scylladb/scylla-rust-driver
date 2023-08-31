pub mod partitioner;
pub mod sharding;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub struct Token {
    pub value: i64,
}
