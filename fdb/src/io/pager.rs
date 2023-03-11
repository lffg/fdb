use std::{
    collections::hash_map::RandomState,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use buff::Buff;
use tokio::sync::{
    mpsc::{self},
    Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
};
use tracing::{debug, info, instrument, trace};

use crate::{
    catalog::page::{FirstPage, Page, PageId, SpecificPage},
    error::{DbResult, Error},
    io::{cache::Cache, disk_manager::DiskManager},
    util::io::Serde,
};

type LockedPage = RwLock<Page>;

type PageNotification = (PageId, PageRefType);
type PageNotificationSender = mpsc::UnboundedSender<PageNotification>;
type PageNotificationReceiver = mpsc::UnboundedReceiver<PageNotification>;

pub struct Pager {
    /// The page size.
    page_size: u16,
    /// The underlying disk manager.
    disk_manager: Mutex<DiskManager>,
    /// The page cache to help avoid doing unnecessary disk accesses.
    ///
    /// XX: Deal with conflicts related to the eviction of an in-use page, which
    /// could lead to two **different** references (and RwLocks) to the same
    /// page. One *maybe* could use some kind of checksum verification to ensure
    /// the serial requirements of page write sequences.
    cache: Cache<PageId, LockedPage>,
    /// Page guard drop sender.
    page_status_tx: PageNotificationSender,
    /// Page guard drop receiver.
    page_status_rx: Mutex<PageNotificationReceiver>,
}

impl Pager {
    /// Constructs a new pager.
    pub fn new(disk_manager: DiskManager) -> Pager {
        let page_size = disk_manager.page_size();

        let (page_status_tx, rx) = mpsc::unbounded_channel::<PageNotification>();
        let page_status_rx = Mutex::new(rx);
        let disk_manager = Mutex::new(disk_manager);

        Pager {
            page_size,
            cache: Cache::new(8192, RandomState::default()),
            disk_manager,
            page_status_tx,
            page_status_rx,
        }
    }

    /// Returns the database's page size.
    pub fn page_size(&self) -> u16 {
        self.page_size
    }

    /// Returns a [`PagerGuard`] for the given page ID. This guard may be used
    /// to lock the page for a write or for a read.
    pub async fn get<S: SpecificPage>(&self, page_id: PageId) -> DbResult<PagerGuard<S>> {
        let inner = self
            .cache
            .get_or_load::<_, Error>(page_id, async {
                let page = self.disk_read_page(page_id).await?;
                Ok(RwLock::new(page))
            })
            .await?;
        Ok(PagerGuard {
            inner,
            notifier: self.page_status_tx.clone(),
            _specific: PhantomData,
        })
    }

    /// Flushes all available pages.
    // XX: Review this design, which imposes read-only queries to call
    // `flush_all` in order to clean the used records from `in_use`. Ideally,
    // such a map's READ entries should be removed when the guard drops.
    #[instrument(level = "debug", skip_all)]
    pub async fn flush_all(&self) -> DbResult<()> {
        // TODO: Use a buffer pool.
        let mut buf = vec![0; self.page_size as usize];

        let mut rx = self.page_status_rx.lock().await;
        let mut flush_count = 0;

        loop {
            let Ok((page_id, ref_type)) = rx.try_recv() else {
                debug!("flushed {flush_count} pages");
                return Ok(());
            };

            let page_arc = self.cache.get(&page_id).await.expect("page must exist");

            if ref_type == PageRefType::Write {
                let mut buf = Buff::new(&mut buf);

                {
                    // In write reads, this lock should not have any contention.
                    let page = page_arc.read().await;

                    // TODO: FIXME: A failure in serialization may incur in
                    // database file corruption. For example, if page A was
                    // successfully written in an INSERT sequence (A -> B -> C)
                    // but B failed during serialization, the DB becomes
                    // inconsistent since A was written, but B and C were not.
                    page.serialize(&mut buf)?;

                    // `serialize` should fill the buffer.
                    debug_assert_eq!(buf.remaining(), 0);
                }

                {
                    // Write contents. The comment above also applies here.
                    self.disk_manager
                        .lock()
                        .await
                        .write_page(page_id, buf.get())
                        .await?;
                    debug!(?page_id, "flushed page to disk");
                }

                flush_count += 1;
            }
        }
    }

    /// Allocates a new page, returning a [`PagerGuard`] to it. The page is
    /// flushed.
    ///
    /// # Deadlock
    ///
    /// This method acquires a write latch to the first page. Hence, callers
    /// must guarantee that there are no other active guards (read or write) to
    /// the first page.
    #[instrument(level = "debug", skip_all)]
    #[must_use]
    pub async fn alloc<S, F>(&self, create: F) -> DbResult<PagerGuard<S>>
    where
        S: SpecificPage,
        F: FnOnce(u16, PageId) -> S,
    {
        debug!(ty = ?S::ty(), "allocating page");

        let first_page_guard = self.get::<FirstPage>(PageId::new_u32(1)).await?;
        let mut first_page = first_page_guard.write().await;

        first_page.header.page_count += 1;

        let page_id = PageId::new_u32(first_page.header.page_count);
        let init = create(self.page_size, page_id);

        let mut buf = vec![0; self.page_size as usize];
        self.flush_page(&mut buf, &init).await?;

        debug!("flushing first page metadata...");
        first_page.flush();

        let guard_inner = Arc::new(RwLock::new(init.into_page()));
        self.cache
            .insert_new(page_id, Arc::clone(&guard_inner))
            .await;
        debug!(?page_id, "page allocated");

        Ok(PagerGuard {
            inner: guard_inner,
            notifier: self.page_status_tx.clone(),
            _specific: PhantomData,
        })
    }

    /// Writes the given page to the database.
    ///
    /// Callers must ensure consistency with the main database header.
    #[instrument(level = "debug", skip_all)]
    async fn flush_page(&self, buf: &mut [u8], page: &impl SpecificPage) -> DbResult<()> {
        let mut buf = Buff::new(buf);

        // TODO: FIXME: A failure in serialization may incur in
        // database file corruption. For example, if page A was
        // successfully written in an INSERT sequence (A -> B -> C)
        // but B failed during serialization, the DB becomes
        // inconsistent since A was written, but B and C were not.
        //                      \/
        page.serialize(&mut buf)?;
        // `serialize` should fill the buffer.
        debug_assert_eq!(buf.remaining(), 0);

        let id = page.id();
        debug!(?id, "will flush now");

        self.disk_manager
            .lock()
            .await
            .write_page(id, buf.get())
            // Same remarks from serialization applies here.
            //    \/
            .await?;

        Ok(())
    }

    /// Writes the given page to the database.
    ///
    /// # Safety
    ///
    /// Callers must ensure consistency with the main database header.
    pub async unsafe fn flush_page_and_build_guard<S>(&self, page: S) -> DbResult<PagerGuard<S>>
    where
        S: SpecificPage,
    {
        let mut buf = vec![0; self.page_size as usize];
        self.flush_page(&mut buf, &page).await?;

        let id = page.id();
        let inner = Arc::new(RwLock::new(page.into_page()));
        self.cache.insert_new(id, Arc::clone(&inner)).await;

        Ok(PagerGuard {
            inner,
            notifier: self.page_status_tx.clone(),
            _specific: PhantomData,
        })
    }

    /// Clears all cache information associated with the given page ID.
    ///
    /// # Safety
    ///
    /// Callers must ensure that there are no other alive references to the
    /// given page.
    pub async unsafe fn clear_cache(&self, page_id: PageId) {
        self.cache.evict(&page_id).await;
    }

    /// Loads the page from the disk.
    async fn disk_read_page(&self, page_id: PageId) -> DbResult<Page> {
        // TODO: Use a buffer pool.
        let mut buf = vec![0; self.page_size as usize];
        let mut buf = Buff::new(&mut buf);

        {
            let mut dm = self.disk_manager.lock().await;
            dm.read_page(page_id, buf.get_mut()).await?;
        }

        Page::deserialize(&mut buf)
    }
}

/// A page guard over a specific page type of type `S`.
pub struct PagerGuard<S>
where
    S: SpecificPage,
{
    inner: Arc<LockedPage>,
    notifier: PageNotificationSender,
    _specific: PhantomData<S>,
}

impl<S> PagerGuard<S>
where
    S: SpecificPage,
{
    /// Locks the page for reading. As the underlying lock is a `RwLock`, other
    /// read references may also exist at the same time.
    #[instrument(level = "trace", skip_all)]
    pub async fn read(&self) -> PagerReadGuard<'_, S> {
        let guard = self.inner.read().await;
        trace!(page_id = ?guard.id(), ty = ?S::ty(), "acquiring read guard");
        PagerReadGuard {
            guard,
            notifier: self.notifier.clone(),
            manually_dropped: false,
            _specific: PhantomData,
        }
    }

    /// Locks the page for writing. There may be no other references (read or
    /// write) concurrently.
    #[instrument(level = "trace", skip_all)]
    pub async fn write(&self) -> PagerWriteGuard<'_, S> {
        let guard = self.inner.write().await;
        trace!(page_id = ?guard.id(), ty = ?S::ty(), "acquiring write guard");
        PagerWriteGuard {
            guard,
            notifier: self.notifier.clone(),
            manually_dropped: false,
            _specific: PhantomData,
        }
    }
}

/// A page read guard. Non-exclusive for other read guards.
pub struct PagerReadGuard<'a, S> {
    guard: RwLockReadGuard<'a, Page>,
    notifier: PageNotificationSender,
    manually_dropped: bool,
    _specific: PhantomData<S>,
}

impl<S> PagerReadGuard<'_, S>
where
    S: SpecificPage,
{
    /// Releases the page reference guard.
    pub fn release(mut self) {
        self.notifier
            .send((self.guard.id(), PageRefType::Read))
            .expect("receiver must be alive");
        self.manually_dropped = true;
        trace!(ty = ?S::ty(), "released read guard");
    }
}

impl<S> Deref for PagerReadGuard<'_, S>
where
    S: SpecificPage,
{
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.guard.cast_ref()
    }
}

impl<S> Drop for PagerReadGuard<'_, S> {
    fn drop(&mut self) {
        let page_id = self.guard.id();
        if !self.manually_dropped {
            info!(?page_id, "did not release read pager guard");
        }
    }
}

/// A page write guard. Exclusive.
pub struct PagerWriteGuard<'a, S> {
    guard: RwLockWriteGuard<'a, Page>,
    notifier: PageNotificationSender,
    manually_dropped: bool,
    _specific: PhantomData<S>,
}

impl<S> PagerWriteGuard<'_, S>
where
    S: SpecificPage,
{
    /// Releases the page reference guard and **schedules** a flush.
    pub fn flush(mut self) {
        self.notifier
            .send((self.guard.id(), PageRefType::Write))
            .expect("receiver must be alive");
        self.manually_dropped = true;
        debug!(ty = ?S::ty(), "flushed write guard");
    }
}

impl<S> Deref for PagerWriteGuard<'_, S>
where
    S: SpecificPage,
{
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.guard.cast_ref()
    }
}

impl<S> DerefMut for PagerWriteGuard<'_, S>
where
    S: SpecificPage,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.cast_mut()
    }
}

impl<S> Drop for PagerWriteGuard<'_, S> {
    fn drop(&mut self) {
        if !self.manually_dropped {
            let page_id = self.guard.id();
            // TODO: Handle this with more robustness.
            info!(?page_id, "did not flush write pager guard");
        }
    }
}

/// The page reference type.
#[derive(Debug, PartialEq, Eq)]
enum PageRefType {
    Read,
    Write,
}
