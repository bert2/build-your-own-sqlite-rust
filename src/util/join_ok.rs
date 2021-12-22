use anyhow::Result;
use std::fmt::{Display, Write};

// code adapted from https://docs.rs/itertools/0.10.3/src/itertools/lib.rs.html#2059-2076
pub trait JoinOkExt: Iterator {
    fn join_ok<D>(&mut self, sep: &str) -> Result<String>
    where
        Self: Iterator<Item = Result<D>> + Sized,
        D: Display,
    {
        match self.next() {
            None => Ok(String::new()),
            Some(first_elt) => {
                // estimate lower bound of capacity needed
                let (lower, _) = self.size_hint();
                let mut result = String::with_capacity(sep.len() * lower);

                write!(&mut result, "{}", first_elt?).unwrap();

                for elt in self {
                    result.push_str(sep);
                    write!(&mut result, "{}", elt?).unwrap();
                }

                Ok(result)
            }
        }
    }
}

impl<I: Iterator> JoinOkExt for I {}
