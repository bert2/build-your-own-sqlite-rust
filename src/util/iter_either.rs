use std::{
    iter::{Chain, Flatten},
    option::IntoIter,
};

type Chained<L, R> = Chain<Flatten<IntoIter<L>>, Flatten<IntoIter<R>>>;

pub struct IterEither<L, R, T>
where
    L: Iterator<Item = T>,
    R: Iterator<Item = T>,
{
    pub left: Option<L>,
    pub right: Option<R>,
    inner: Option<Chained<L, R>>,
}

impl<L, R, T> IterEither<L, R, T>
where
    L: Iterator<Item = T>,
    R: Iterator<Item = T>,
{
    pub fn left(iter: L) -> Self {
        Self {
            left: Some(iter),
            right: None,
            inner: None,
        }
    }

    pub fn right(iter: R) -> Self {
        Self {
            left: None,
            right: Some(iter),
            inner: None,
        }
    }
}

impl<L, R, T> Iterator for IterEither<L, R, T>
where
    L: Iterator<Item = T>,
    R: Iterator<Item = T>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if matches!(self.inner, None) {
            self.inner = Some(
                self.left
                    .take()
                    .into_iter()
                    .flatten()
                    .chain(self.right.take().into_iter().flatten()),
            );
        }

        self.inner.as_mut().unwrap().next()
    }
}
