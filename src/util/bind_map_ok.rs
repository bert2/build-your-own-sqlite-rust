pub trait BindMapOkExt {
    fn bind_map_ok<F, T, U, E>(self, f: F) -> BindMapOk<Self, F>
    where
        Self: Iterator<Item = Result<T, E>> + Sized,
        F: FnMut(T) -> Result<U, E>,
    {
        BindMapOk { iter: self, f }
    }
}

impl<I> BindMapOkExt for I {}

pub struct BindMapOk<I, F> {
    iter: I,
    f: F,
}

impl<I, F, T, U, E> Iterator for BindMapOk<I, F>
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
        let mut iter = xs.into_iter().bind_map_ok(inv);

        assert_eq!(iter.next(), Some(Err("div by zero")));
        assert_eq!(iter.next(), Some(Err("oof")));
        assert_eq!(iter.next(), Some(Ok(0.5)));
    }
}
