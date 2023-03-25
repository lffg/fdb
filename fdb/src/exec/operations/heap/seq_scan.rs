use std::marker::PhantomData;

use tracing::{instrument, trace};

use crate::{
    catalog::page::{HeapPage, PageId, SpecificPage},
    error::DbResult,
    exec::{operations::PhysicalState, util::macros::get_or_insert_with},
    util::io::Size,
    Db,
};

pub struct SeqScan<T> {
    first_page_id: PageId,
    state: Option<State>,
    _type: PhantomData<T>,
}

struct State {
    page_id: PageId,
    next_page_id: Option<PageId>,
    rem_total: u64,
    rem_page: u16,
    offset: u16,
}

impl<T> SeqScan<T> {
    /// Constructs a new heap page sequence scanner.
    pub fn new(first_page_id: PageId) -> Self {
        SeqScan {
            first_page_id,
            state: None,
            _type: PhantomData,
        }
    }

    /// Returns the current element and advances the underlying iterator.
    pub async fn next<De>(&mut self, db: &Db, deserializer: De) -> DbResult<Option<T>>
    where
        De: Fn(&mut buff::Buff, PhysicalState) -> DbResult<T>,
        T: Size,
    {
        let (state, maybe_record) = self.load(db, deserializer).await?;
        if let Some(record) = &maybe_record {
            state.offset += record.size() as u16;
            state.rem_total -= 1;
            state.rem_page -= 1;
        }
        Ok(maybe_record)
    }

    /// Returns the current element without advancing the underlying iterator.
    ///
    /// This method doesn't perform any kind of cache, which is handled by the
    /// underlying database pager.
    pub async fn peek<De>(&mut self, db: &Db, deserializer: De) -> DbResult<Option<T>>
    where
        De: Fn(&mut buff::Buff, PhysicalState) -> DbResult<T>,
    {
        self.load(db, deserializer)
            .await
            .map(|(_, maybe_record)| maybe_record)
    }

    /// Load record implementation. Though it changes the state on page
    /// switches, it doesn't advance the record counters when a record is
    /// deserialized.
    #[instrument(level = "debug", skip_all)]
    async fn load<De>(&mut self, db: &Db, deserializer: De) -> DbResult<(&mut State, Option<T>)>
    where
        De: Fn(&mut buff::Buff, PhysicalState) -> DbResult<T>,
    {
        let state = get_or_insert_with!(&mut self.state, || {
            let first_page_id = self.first_page_id;
            trace!(?first_page_id, "loading first page of sequence");

            db.pager()
                .read_with(first_page_id, |page: &HeapPage| {
                    let seq_header = page.header.seq_header.as_ref().expect("first seq page");

                    State {
                        page_id: first_page_id,
                        next_page_id: page.header.next_page_id,
                        rem_total: seq_header.record_count,
                        rem_page: page.header.record_count,
                        offset: page.first_offset(),
                    }
                })
                .await?
        });

        if state.rem_total == 0 {
            trace!("no more entries in sequence, done");
            return Ok((state, None));
        }

        if state.rem_page == 0 {
            let next_page_id = state.next_page_id.expect("must have +1");
            trace!(?next_page_id, "loading next page of sequence");
            db.pager()
                .read_with(next_page_id, |page: &HeapPage| {
                    state.page_id = page.id();
                    state.next_page_id = page.header.next_page_id;
                    state.rem_page = page.header.record_count;
                    state.offset = page.first_offset();
                })
                .await?;
        }

        trace!("deserializing record using provided deserializer");
        let physical_state = PhysicalState {
            page_id: state.page_id,
            offset: state.offset,
        };
        let record = db
            .pager()
            .read_with(state.page_id, |page: &HeapPage| {
                page.read_at(state.offset, |buf| {
                    // Deserializes the record:
                    deserializer(buf, physical_state)
                })
            })
            .await??;
        Ok((state, Some(record)))
    }
}
