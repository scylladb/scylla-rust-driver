use crate::routing::Token;
use std::borrow::Borrow;

#[derive(Clone)]
pub struct TokenRing<ElemT> {
    ring: Vec<(Token, ElemT)>,
}

impl<ElemT> TokenRing<ElemT> {
    pub fn new(ring_iter: impl Iterator<Item = (Token, ElemT)>) -> TokenRing<ElemT> {
        let mut ring: Vec<(Token, ElemT)> = ring_iter.collect();
        ring.sort_by(|a, b| a.0.cmp(&b.0));
        TokenRing { ring }
    }

    pub fn iter(&self) -> impl Iterator<Item = &(Token, ElemT)> {
        self.ring.iter()
    }

    pub fn ring_range_full(
        &self,
        token: impl Borrow<Token>,
    ) -> impl Iterator<Item = &(Token, ElemT)> {
        let binary_search_index: usize =
            match self.ring.binary_search_by(|e| e.0.cmp(token.borrow())) {
                Ok(exact_match_index) => exact_match_index,
                Err(first_greater_index) => first_greater_index,
            };

        self.ring[binary_search_index..]
            .iter()
            .chain(self.ring.iter())
            .take(self.ring.len())
    }

    pub fn ring_range(&self, token: impl Borrow<Token>) -> impl Iterator<Item = &ElemT> {
        self.ring_range_full(token).map(|(_t, e)| e)
    }

    pub fn get_elem_for_token(&self, token: impl Borrow<Token>) -> Option<&ElemT> {
        self.ring_range(token).next()
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
