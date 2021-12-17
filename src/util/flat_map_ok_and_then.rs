pub trait FlatMapOkAndThenExt {
    fn flat_map_ok_and_then<F, T, U, E, J>(self, f: F) -> FlatMapOkAndThen<Self, F, J>
    where
        Self: Iterator<Item = Result<T, E>> + Sized,
        F: FnMut(T) -> J,
        J: Iterator<Item = Result<U, E>>,
    {
        FlatMapOkAndThen {
            iter: self,
            f,
            inner: None,
        }
    }
}

impl<I> FlatMapOkAndThenExt for I {}

pub struct FlatMapOkAndThen<I, F, J> {
    iter: I,
    f: F,
    inner: Option<J>,
}

impl<I, F, T, U, E, J> Iterator for FlatMapOkAndThen<I, F, J>
where
    I: Iterator<Item = Result<T, E>>,
    F: FnMut(T) -> J,
    J: Iterator<Item = Result<U, E>>,
{
    type Item = Result<U, E>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(inner) = &mut self.inner {
                if let Some(item) = inner.next() {
                    return Some(item);
                }
            }

            match self.iter.next() {
                Some(Ok(x)) => self.inner = Some((self.f)(x)),
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter::repeat;

    #[test]
    fn test() {
        let repeat = |x: usize| repeat(x).take(x).map(Ok);

        let xs = vec![Ok(2), Err("oof"), Ok(3)];
        let mut iter = xs.into_iter().flat_map_ok_and_then(repeat);

        assert_eq!(iter.next(), Some(Ok(2)));
        assert_eq!(iter.next(), Some(Ok(2)));
        assert_eq!(iter.next(), Some(Err("oof")));
        assert_eq!(iter.next(), Some(Ok(3)));
        assert_eq!(iter.next(), Some(Ok(3)));
        assert_eq!(iter.next(), Some(Ok(3)));
        assert_eq!(iter.next(), None);
    }
}
