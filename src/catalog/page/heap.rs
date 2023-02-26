use crate::{
    catalog::page::{Page, PageId},
    error::DbResult,
    ioutil::{BuffExt, Serde},
};

/// Heap page. Stores records in an unordered manner.
#[derive(Debug)]
pub struct HeapPage {
    pub id: PageId,
    pub next_page_id: Option<PageId>,
    pub bytes: Vec<u8>, // TODO: Review this.
}

impl Page for HeapPage {
    fn id(&self) -> PageId {
        self.id
    }
}

impl Serde for HeapPage {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        buf.write_page_id(Some(self.id));
        buf.write_page_id(self.next_page_id);
        buf.write_slice(&self.bytes);
        Ok(())
    }

    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let id = buf.read_page_id().expect("current page id");
        let next_page_id = buf.read_page_id();

        let mut bytes = vec![0; buf.remaining()]; // TODO: Optimize using `MaybeUninit`.
        buf.read_slice(&mut bytes);

        Ok(HeapPage {
            id,
            next_page_id,
            bytes,
        })
    }
}
