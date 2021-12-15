// Code stolen from https://github.com/hdevalke/take-until

use core::fmt;
use core::iter::FusedIterator;

/// TakeUntilExt is an extension trait for iterators.
/// It adds the `take_until` method.
pub trait TakeUntilExt<P>
where
    Self: Sized,
{
    /// Takes items until the predicate is true,
    /// including the item that made the predicate true.
    fn take_until(self, predicate: P) -> TakeUntil<Self, P>;
}

impl<I, P> TakeUntilExt<P> for I
where
    I: Sized + Iterator,
    P: FnMut(&I::Item) -> bool,
{
    fn take_until(self, predicate: P) -> TakeUntil<Self, P> {
        TakeUntil {
            iter: self,
            flag: false,
            predicate,
        }
    }
}
/// TakeUntil is similar to the TakeWhile iterator,
/// but takes items until the predicate is true,
/// including the item that made the predicate true.
pub struct TakeUntil<I, P> {
    iter: I,
    flag: bool,
    predicate: P,
}

impl<I: fmt::Debug, P> fmt::Debug for TakeUntil<I, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TakeUntil")
            .field("iter", &self.iter)
            .field("flag", &self.flag)
            .finish()
    }
}

impl<I, P> Iterator for TakeUntil<I, P>
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
                if (self.predicate)(&x) {
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

impl<I, P> FusedIterator for TakeUntil<I, P>
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
        let mut iter = v.iter().take_until(|_| true);
        assert_eq!((0, Some(3)), iter.size_hint());
        iter.next();
        assert_eq!((0, Some(0)), iter.size_hint());
    }
}
