pub fn opt_contains<T>(o: &Option<T>, x: &T) -> bool
where
    T: PartialEq,
{
    match o {
        Some(y) => y == x,
        None => false,
    }
}

pub fn flip<A, B>((a, b): (A, B)) -> (B, A) {
    (b, a)
}
