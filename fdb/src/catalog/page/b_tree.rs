//! Heap pages store records in an unordered and sequential fashion.

use std::ops::Add;

use crate::{
    catalog::page::{Page, PageId, PageType, SpecificPage},
    error::DbResult,
    util::io::{Deserialize, Serialize, Size},
};

#[derive(Debug)]
pub enum BTreePage {
    Internal(BTreeInternalPage), // tag OxAA
    Leaf(BTreeLeafPage),         // tag 0xFF
}

impl Size for BTreePage {
    fn size(&self) -> u32 {
        (1_u32) // top-level page type (btree)
            .add(1) // btree type tag
            .add(4) // page id
            .add(2) // cell_count
            .add(match self {
                BTreePage::Internal(node) => 4 * node.ptrs.len() + node.keys.len(),
                BTreePage::Leaf(node) => 4 + 4 + node.cells.len(),
            } as u32)
    }
}

impl Serialize for BTreePage {
    fn serialize(&self, buf: &mut buff::Buff<'_>) -> DbResult<()> {
        PageType::BTree.serialize(buf)?; // top-level page type (btree)
        match self {
            BTreePage::Internal(node) => {
                buf.write(0xAA_u8); // tag for internal page
                node.id.serialize(buf)?;
                buf.write(node.cell_count);

                for ptr in &node.ptrs {
                    ptr.serialize(buf)?;
                }
                buf.write_slice(&node.keys);
            }
            BTreePage::Leaf(node) => {
                buf.write(0xFF_u8); // tag for leaf page
                node.id.serialize(buf)?;
                buf.write(node.cell_count);

                node.prev.serialize(buf)?;
                node.next.serialize(buf)?;
                buf.write_slice(&node.cells);
            }
        }
        Ok(())
    }
}

impl Deserialize<'_> for BTreePage {
    fn deserialize(buf: &mut buff::Buff<'_>) -> DbResult<Self>
    where
        Self: Sized,
    {
        let ty = PageType::deserialize(buf)?;
        debug_assert_eq!(ty, PageType::BTree);
        let btree_node_type_tag: u8 = buf.read();
        let id = PageId::deserialize(buf)?;
        let cell_count: u16 = buf.read();
        Ok(match btree_node_type_tag {
            // internal page
            0xAA => BTreePage::Internal(BTreeInternalPage {
                id,
                cell_count,
                ptrs: {
                    // `+1` to account for the last pointer
                    let mut ptrs = Vec::with_capacity((cell_count + 1) as usize);
                    for _ in 0..(cell_count + 1) {
                        ptrs.push(PageId::deserialize(buf)?);
                    }
                    ptrs
                },
                keys: {
                    let mut bytes = vec![0; buf.remaining()];
                    buf.read_slice(&mut bytes);
                    bytes
                },
            }),
            // leaf page
            0xFF => BTreePage::Leaf(BTreeLeafPage {
                id,
                cell_count,
                prev: Option::<PageId>::deserialize(buf)?,
                next: Option::<PageId>::deserialize(buf)?,
                cells: {
                    let mut bytes = vec![0; buf.remaining()];
                    buf.read_slice(&mut bytes);
                    bytes
                },
            }),
            _ => panic!("corrupted file or impl bug"),
        })
    }
}

impl SpecificPage for BTreePage {
    fn ty() -> PageType {
        PageType::BTree
    }

    fn id(&self) -> PageId {
        match self {
            BTreePage::Internal(inner) => inner.id,
            BTreePage::Leaf(inner) => inner.id,
        }
    }

    super::impl_cast_methods!(Page::BTree => BTreePage);
}

#[derive(Debug)]
pub struct BTreeInternalPage {
    id: PageId,
    cell_count: u16,
    ptrs: Vec<PageId>,
    keys: Vec<u8>,
}

#[derive(Debug)]
pub struct BTreeLeafPage {
    id: PageId,
    cell_count: u16,
    prev: Option<PageId>,
    next: Option<PageId>,
    cells: Vec<u8>,
}
