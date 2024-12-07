use itertools::Either;

use crate::cluster::{Node, NodeRef};
use std::{ops::Index, sync::Arc};

/// Container holding replicas, enabling unified use of both borrowed and owned `Node` sequences.
///
/// This type is very similar to `Cow`, but unlike `Cow`,
/// it holds references in an `Owned` variant `Vec`.
#[derive(Debug)]
pub(crate) enum ReplicasArray<'a> {
    Borrowed(&'a [Arc<Node>]),
    Owned(Vec<NodeRef<'a>>),
}

impl<'a> ReplicasArray<'a> {
    pub(crate) fn get(&self, index: usize) -> Option<NodeRef<'a>> {
        match self {
            ReplicasArray::Borrowed(slice) => slice.get(index),
            ReplicasArray::Owned(vec) => vec.get(index).copied(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            ReplicasArray::Borrowed(slice) => slice.len(),
            ReplicasArray::Owned(vec) => vec.len(),
        }
    }

    pub(crate) fn iter<'s>(&'s self) -> impl Iterator<Item = NodeRef<'a>> + 's {
        match self {
            ReplicasArray::Borrowed(slice) => Either::Left(slice.iter()),
            ReplicasArray::Owned(vec) => Either::Right(vec.iter().copied()),
        }
    }
}

impl<'a> FromIterator<NodeRef<'a>> for ReplicasArray<'a> {
    fn from_iter<T: IntoIterator<Item = NodeRef<'a>>>(iter: T) -> Self {
        Self::Owned(iter.into_iter().collect())
    }
}

impl<'a> From<&'a [Arc<Node>]> for ReplicasArray<'a> {
    fn from(item: &'a [Arc<Node>]) -> Self {
        Self::Borrowed(item)
    }
}

impl Index<usize> for ReplicasArray<'_> {
    type Output = Arc<Node>;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            ReplicasArray::Borrowed(b) => &b[index],
            ReplicasArray::Owned(o) => o[index],
        }
    }
}

pub(crate) const EMPTY_REPLICAS: ReplicasArray<'static> = ReplicasArray::Borrowed(&[]);
