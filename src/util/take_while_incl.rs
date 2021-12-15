// Code stolen and adapted from https://github.com/hdevalke/take-until

use core::fmt;
use core::iter::FusedIterator;

/// TakeWhileInclExt is an extension trait for iterators.
/// It adds the `take_while_incl` method.
pub trait TakeWhileInclExt<P>
where
    Self: Sized,
{
    /// Takes items while the predicate is true,
    /// including the item that made the predicate false.
    fn take_while_incl(self, predicate: P) -> TakeWhileIncl<Self, P>;
}

impl<I, P> TakeWhileInclExt<P> for I
where
    I: Sized + Iterator,
    P: FnMut(&I::Item) -> bool,
{
    fn take_while_incl(self, predicate: P) -> TakeWhileIncl<Self, P> {
        TakeWhileIncl {
            iter: self,
            flag: false,
            predicate,
        }
    }
}
/// TakeWhileIncl is similar to the TakeWhile iterator.
/// It takes items while the predicate is true, but
/// including the item that made the predicate false.
pub struct TakeWhileIncl<I, P> {
    iter: I,
    flag: bool,
    predicate: P,
}

impl<I: fmt::Debug, P> fmt::Debug for TakeWhileIncl<I, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TakeWhileIncl")
            .field("iter", &self.iter)
            .field("flag", &self.flag)
            .finish()
    }
}

impl<I, P> Iterator for TakeWhileIncl<I, P>
where
    I: Iterator,
    P: FnMut(&I::Item) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<I::Item> {
        if self.flag {
            None
        } else {
            self.iter.next().and_then(|x| {
                if !(self.predicate)(&x) {
                    self.flag = true;
                }
                Some(x)
            })
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.flag {
            (0, Some(0))
        } else {
            let (_, upper) = self.iter.size_hint();
            (0, upper) // can't know a lower bound, due to the predicate
        }
    }
}

impl<I, P> FusedIterator for TakeWhileIncl<I, P>
where
    I: FusedIterator,
    P: FnMut(&I::Item) -> bool,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_size_hint_zero() {
        let v: Vec<u8> = vec![0, 1, 2];
        let mut iter = v.iter().take_while_incl(|_| false);
        assert_eq!((0, Some(3)), iter.size_hint());
        iter.next();
        assert_eq!((0, Some(0)), iter.size_hint());
    }
}
