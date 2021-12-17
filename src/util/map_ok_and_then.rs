pub trait MapOkAndThenExt {
    fn map_ok_and_then<F, T, U, E>(self, f: F) -> MapOkAndThen<Self, F>
    where
        Self: Iterator<Item = Result<T, E>> + Sized,
        F: FnMut(T) -> Result<U, E>,
    {
        MapOkAndThen { iter: self, f }
    }
}

impl<I> MapOkAndThenExt for I {}

pub struct MapOkAndThen<I, F> {
    iter: I,
    f: F,
}

impl<I, F, T, U, E> Iterator for MapOkAndThen<I, F>
where
    I: Iterator<Item = Result<T, E>>,
    F: FnMut(T) -> Result<U, E>,
{
    type Item = Result<U, E>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|r| r.and_then(|x| (self.f)(x)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let inv = |x: i32| match x {
            0 => Err("div by zero"),
            _ => Ok(1. / x as f64),
        };

        let xs = vec![Ok(0), Err("oof"), Ok(2)];
        let mut iter = xs.into_iter().map_ok_and_then(inv);

        assert_eq!(iter.next(), Some(Err("div by zero")));
        assert_eq!(iter.next(), Some(Err("oof")));
        assert_eq!(iter.next(), Some(Ok(0.5)));
    }
}
