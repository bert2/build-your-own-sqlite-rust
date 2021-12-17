pub trait MapOkExt {
    fn map_ok<F, T, U, E>(self, f: F) -> MapOk<Self, F>
    where
        Self: Iterator<Item = Result<T, E>> + Sized,
        F: FnMut(T) -> U,
    {
        MapOk { iter: self, f }
    }
}

impl<I> MapOkExt for I {}

pub struct MapOk<I, F> {
    iter: I,
    f: F,
}

impl<I, F, T, U, E> Iterator for MapOk<I, F>
where
    I: Iterator<Item = Result<T, E>>,
    F: FnMut(T) -> U,
{
    type Item = Result<U, E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|r| r.map(|x| (self.f)(x)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let inv = |x: i32| 1. / x as f32;

        let xs = vec![Ok(1), Err("oof"), Ok(3)];
        let mut iter = xs.into_iter().map_ok(inv);

        assert_eq!(iter.next(), Some(Ok(1.0)));
        assert_eq!(iter.next(), Some(Err("oof")));
        assert_eq!(iter.next(), Some(Ok(1. / 3.)));
    }
}
