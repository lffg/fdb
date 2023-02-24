use std::{
    fs::OpenOptions,
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::{
    disk_manager::DiskManager,
    page::{FirstPage, PageId},
    pager::Pager,
};

mod catalog;
mod config;
mod page;

mod disk_manager;
mod pager;

fn main() -> io::Result<()> {
    let disk_manager = DiskManager::new(Path::new("ignore/my-db"))?;
    let mut pager = Pager::new(disk_manager);

    let first_page: FirstPage = pager.load(PageId::new(1.try_into().unwrap())).unwrap();
    dbg!(first_page);

    Ok(())
}
