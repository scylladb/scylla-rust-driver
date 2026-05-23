use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap},
    hash::{BuildHasher, Hash},
};

pub(crate) trait StringKeyedMapLike {
    type Value;

    fn get(&self, key: &str) -> Option<&Self::Value>;
}

impl<K, T> StringKeyedMapLike for BTreeMap<K, T>
where
    K: Borrow<str> + Ord,
{
    type Value = T;

    fn get(&self, key: &str) -> Option<&Self::Value> {
        Self::get(self, key)
    }
}

impl<K, T, S: BuildHasher> StringKeyedMapLike for HashMap<K, T, S>
where
    K: Borrow<str> + Eq + Hash,
{
    type Value = T;

    fn get(&self, key: &str) -> Option<&Self::Value> {
        Self::get(self, key)
    }
}
