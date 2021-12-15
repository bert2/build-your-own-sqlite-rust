pub trait ContainsExt<T> {
    fn contains_<U>(&self, x: &U) -> bool
    where
        T: PartialEq<U>;
}

impl<T> ContainsExt<T> for Option<T> {
    fn contains_<U>(&self, x: &U) -> bool
    where
        T: PartialEq<U>,
    {
        match self {
            Some(y) => y == x,
            None => false,
        }
    }
}
