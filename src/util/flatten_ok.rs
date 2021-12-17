// Code stolen and adapted from https://github.com/rust-itertools/itertools/blob/125502571abfe49b619bc9ce0adde47414a1a601/src/flatten_ok.rs#L7

pub trait FlattenOkExt {
    fn flatten_ok<T, E>(self) -> FlattenOk<Self, T, E>
    where
        Self: Iterator<Item = Result<T, E>> + Sized,
        T: IntoIterator,
    {
        FlattenOk {
            iter: self,
            inner: None,
        }
    }
}

impl<I> FlattenOkExt for I {}

pub struct FlattenOk<I, T, E>
where
    I: Iterator<Item = Result<T, E>>,
    T: IntoIterator,
{
    iter: I,
    inner: Option<T::IntoIter>,
}

impl<I, T, E> Iterator for FlattenOk<I, T, E>
where
    I: Iterator<Item = Result<T, E>>,
    T: IntoIterator,
{
    type Item = Result<T::Item, E>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(inner) = &mut self.inner {
                if let Some(item) = inner.next() {
                    return Some(Ok(item));
                }
            }

            match self.iter.next() {
                Some(Ok(x)) => self.inner = Some(x.into_iter()),
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let xs = vec![Ok(1..3), Err("oof"), Ok(3..6)];
        let mut iter = xs.into_iter().flatten_ok();

        assert_eq!(iter.next(), Some(Ok(1)));
        assert_eq!(iter.next(), Some(Ok(2)));
        assert_eq!(iter.next(), Some(Err("oof")));
        assert_eq!(iter.next(), Some(Ok(3)));
        assert_eq!(iter.next(), Some(Ok(4)));
        assert_eq!(iter.next(), Some(Ok(5)));
    }
}
