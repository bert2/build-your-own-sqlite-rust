pub mod bind_map_ok;
mod contains_;
pub mod flatten_ok;
mod flip;
pub mod map_ok;
pub mod str_sim;
mod take_while_incl;

pub use contains_::ContainsExt;
pub use flip::flip;
pub use take_while_incl::TakeWhileInclExt;
