use std::convert::identity as id;

pub trait FlattenExt<T, E> {
    fn flatten_(self) -> Result<T, E>;
}

impl<T, E> FlattenExt<T, E> for Result<Result<T, E>, E> {
    fn flatten_(self) -> Result<T, E> {
        self.and_then(id)
    }
}
