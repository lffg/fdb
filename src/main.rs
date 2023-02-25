use std::path::Path;

use crate::{
    disk_manager::DiskManager,
    error::{DbResult, Error},
    page::{FirstPage, PageId},
    pager::Pager,
};

mod error;

mod catalog;
mod config;
mod page;

mod disk_manager;
mod pager;

mod ioutil;

fn main() -> DbResult<()> {
    let disk_manager = DiskManager::new(Path::new("ignore/my-db"))?;
    let mut pager = Pager::new(disk_manager);

    let first_page = load_first_page(&mut pager)?;
    dbg!(first_page);

    Ok(())
}

fn load_first_page(pager: &mut Pager) -> DbResult<FirstPage> {
    let id = PageId::new(1.try_into().unwrap());

    match pager.load(id) {
        Ok(first_page) => Ok(first_page),
        Err(Error::PageOutOfBounds(_)) => {
            let first_page = FirstPage::default();
            pager.write_flush(&first_page)?;
            Ok(first_page)
        }
        Err(Error::ReadIncompletePage(_)) => {
            panic!("corrupt database file");
        }
        Err(error) => Err(error),
    }
}
