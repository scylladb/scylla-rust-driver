use rand::Rng;
use smallvec::SmallVec;

pub struct RandomOrderIter<T> {
    items_to_try: SmallVec<[T; 4]>,
}

impl<T> FromIterator<T> for RandomOrderIter<T> {
    fn from_iter<I>(iter: I) -> RandomOrderIter<T>
    where
        I: IntoIterator<Item = T>,
    {
        RandomOrderIter {
            items_to_try: iter.into_iter().collect(),
        }
    }
}

impl<T> Iterator for RandomOrderIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.items_to_try.is_empty() {
            return None;
        }

        Some(
            self.items_to_try
                .swap_remove(rand::thread_rng().gen_range(0..self.items_to_try.len())),
        )
    }
}
