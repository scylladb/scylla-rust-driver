use crate::routing::Token;

/// A token ring is a continous hash ring. It defines association by hasing a key onto the ring and the walking the ring in one direction.
/// Cassandra and Scylla use it for determining data ownership which allows for efficient load balancing.
/// The token ring is used by the driver to find the replicas for a given token.
/// Each ring member has a token (i64 number) which defines the member's position on the ring.
/// The ring is circular and can be traversed in the order of increasing tokens.
/// `TokenRing` makes it easy and efficient to traverse the ring starting at a given token.
#[derive(Debug, Clone)]
pub struct TokenRing<ElemT> {
    ring: Vec<(Token, ElemT)>,
}

impl<ElemT> TokenRing<ElemT> {
    pub(crate) const fn new_empty() -> TokenRing<ElemT> {
        Self { ring: Vec::new() }
    }

    pub(crate) fn new(ring_iter: impl Iterator<Item = (Token, ElemT)>) -> TokenRing<ElemT> {
        let mut ring: Vec<(Token, ElemT)> = ring_iter.collect();
        ring.sort_by(|a, b| a.0.cmp(&b.0));
        TokenRing { ring }
    }

    /// Iterates over all members of the ring starting at the lowest token.
    pub fn iter(&self) -> impl Iterator<Item = &(Token, ElemT)> {
        self.ring.iter()
    }

    /// Provides an iterator over the ring members starting at the given token.
    /// The iterator traverses the whole ring in the direction of increasing tokens.
    /// After reaching the maximum token it wraps around and continues from the lowest one.
    /// The iterator visits each member once, it doesn't have an infinte length.
    pub fn ring_range_full(&self, token: Token) -> impl Iterator<Item = &(Token, ElemT)> {
        let binary_search_index: usize = match self.ring.binary_search_by(|e| e.0.cmp(&token)) {
            Ok(exact_match_index) => exact_match_index,
            Err(first_greater_index) => first_greater_index,
        };

        self.ring[binary_search_index..]
            .iter()
            .chain(self.ring.iter())
            .take(self.ring.len())
    }

    /// Provides an iterator over the ring's elements starting at the given token.
    /// The iterator traverses the whole ring in the direction of increasing tokens.
    /// After reaching the maximum token it wraps around and continues from the lowest one.
    /// The iterator visits each member once, it doesn't have an infinte length.
    /// To access the token along with the element you can use `ring_range_full`
    pub fn ring_range(&self, token: Token) -> impl Iterator<Item = &ElemT> {
        self.ring_range_full(token).map(|(_t, e)| e)
    }

    /// Traverses the ring starting at the given token and returns the first ring member encountered.
    pub fn get_elem_for_token(&self, token: Token) -> Option<&ElemT> {
        self.ring_range(token).next()
    }

    /// Get the total number of members in the ring.
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Returns `true` if the token ring contains no elements.
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::TokenRing;
    use crate::routing::Token;

    #[test]
    fn test_token_ring() {
        let ring_data = [
            (Token { value: -30 }, -3),
            (Token { value: -20 }, -2),
            (Token { value: -10 }, -1),
            (Token { value: 0 }, 0),
            (Token { value: 10 }, 1),
            (Token { value: 20 }, 2),
            (Token { value: 30 }, 3),
        ];

        let ring: TokenRing<i32> = TokenRing::new(ring_data.into_iter());

        assert_eq!(
            ring.ring_range(Token { value: -35 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![-3, -2, -1, 0, 1, 2, 3]
        );

        assert_eq!(
            ring.ring_range(Token { value: -30 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![-3, -2, -1, 0, 1, 2, 3]
        );

        assert_eq!(
            ring.ring_range(Token { value: -25 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![-2, -1, 0, 1, 2, 3, -3]
        );

        assert_eq!(
            ring.ring_range(Token { value: -20 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![-2, -1, 0, 1, 2, 3, -3]
        );

        assert_eq!(
            ring.ring_range(Token { value: -15 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![-1, 0, 1, 2, 3, -3, -2]
        );

        assert_eq!(
            ring.ring_range(Token { value: -10 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![-1, 0, 1, 2, 3, -3, -2]
        );

        assert_eq!(
            ring.ring_range(Token { value: -5 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![0, 1, 2, 3, -3, -2, -1]
        );

        assert_eq!(
            ring.ring_range(Token { value: 0 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![0, 1, 2, 3, -3, -2, -1]
        );

        assert_eq!(
            ring.ring_range(Token { value: 5 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![1, 2, 3, -3, -2, -1, 0]
        );

        assert_eq!(
            ring.ring_range(Token { value: 10 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![1, 2, 3, -3, -2, -1, 0]
        );

        assert_eq!(
            ring.ring_range(Token { value: 15 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![2, 3, -3, -2, -1, 0, 1]
        );

        assert_eq!(
            ring.ring_range(Token { value: 20 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![2, 3, -3, -2, -1, 0, 1]
        );

        assert_eq!(
            ring.ring_range(Token { value: 25 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![3, -3, -2, -1, 0, 1, 2]
        );

        assert_eq!(
            ring.ring_range(Token { value: 30 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![3, -3, -2, -1, 0, 1, 2]
        );

        assert_eq!(
            ring.ring_range(Token { value: 35 })
                .cloned()
                .collect::<Vec<i32>>(),
            vec![-3, -2, -1, 0, 1, 2, 3]
        );
    }
}
