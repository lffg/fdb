use async_trait::async_trait;

use crate::{error::DbResult, Db};

pub mod object {
    mod create;
    pub use create::*;

    mod select;
    pub use select::*;
}

pub mod table {
    mod insert;
    pub use insert::*;

    mod select;
    pub use select::*;

    mod delete;
    pub use delete::*;

    mod update;
    pub use update::*;

    // Private-implementation queries.

    mod linear_scan;
    use linear_scan::*;
}

/// Query execution trait. It is implemented for all database operations.
///
/// The database execution is based on the iterator model. This trait expresses
/// such an iterator. The `next` method may be called arbitrarily to lazily
/// fetch records without running out of memory.
#[async_trait]
pub trait Query {
    type Item<'a>;

    /// Produces the next value in the stream.
    async fn next<'a>(&mut self, db: &'a Db) -> DbResult<Option<Self::Item<'a>>>;
}
