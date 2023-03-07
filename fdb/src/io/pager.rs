use std::{
    collections::hash_map::RandomState,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use buff::Buff;
use dashmap::DashMap;
use drop_bomb::DropBomb;
use tokio::sync::{
    mpsc::{self},
    Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
};
use tracing::{info, instrument};

use crate::{
    catalog::page::{FirstPage, Page, PageId, SpecificPage},
    config::PAGE_SIZE,
    error::{DbResult, Error},
    io::{cache::Cache, disk_manager::DiskManager},
    util::io::Serde,
};

type LockedPage = RwLock<Page>;

type PageNotification = (PageId, PageRefType);
type PageNotificationSender = mpsc::UnboundedSender<PageNotification>;
type PageNotificationReceiver = mpsc::UnboundedReceiver<PageNotification>;

pub struct Pager {
    /// The underlying disk manager.
    disk_manager: Mutex<DiskManager>,
    /// The page cache to help avoid doing unnecessary disk accesses.
    cache: Cache<PageId, LockedPage>,
    /// Map to keep track what pages are being used. This is necessary to avoid
    /// conflicts related to the eviction of an in-use page, which could result
    /// in more than two **different** references to the same page.
    ///
    /// By keeping this `in_use` map, the pager doesn't call the cache to fetch
    /// an already-used page.
    in_use: DashMap<PageId, Arc<LockedPage>>,
    /// Page guard drop sender.
    page_status_tx: PageNotificationSender,
    /// Page guard drop receiver.
    page_status_rx: Mutex<PageNotificationReceiver>,
}

impl Pager {
    /// Constructs a new pager.
    pub fn new(disk_manager: DiskManager) -> Pager {
        let (page_status_tx, rx) = mpsc::unbounded_channel::<PageNotification>();
        let page_status_rx = Mutex::new(rx);
        let disk_manager = Mutex::new(disk_manager);
        let in_use = DashMap::<PageId, Arc<LockedPage>>::with_capacity(256);

        Pager {
            cache: Cache::new(8192, RandomState::default()),
            in_use,
            disk_manager,
            page_status_tx,
            page_status_rx,
        }
    }

    /// Returns a [`PagerGuard`] for the given page ID. This guard may be used
    /// to lock the page for a write or for a read.
    pub async fn get<S: SpecificPage>(&self, page_id: PageId) -> DbResult<PagerGuard<S>> {
        let notifier = self.page_status_tx.clone();
        match self.in_use.get(&page_id) {
            Some(page_ref) => Ok(PagerGuard {
                inner: Arc::clone(&page_ref),
                notifier,
                _specific: PhantomData,
            }),
            None => {
                let inner = self
                    .cache
                    .load::<_, Error>(page_id, async {
                        let page = self.disk_read_page(page_id).await?;
                        Ok(RwLock::new(page))
                    })
                    .await?;
                self.in_use.insert(page_id, Arc::clone(&inner));
                Ok(PagerGuard {
                    inner,
                    notifier,
                    _specific: PhantomData,
                })
            }
        }
    }

    /// Flushes all available pages.
    // XX: Review this design, which imposes read-only queries to call
    // `flush_all` in order to clean the used records from `in_use`. Ideally,
    // such a map's READ entries should be removed when the guard drops.
    #[instrument(skip_all)]
    pub async fn flush_all(&self) -> DbResult<()> {
        // TODO: Use a buffer pool.
        let mut buf = Box::new([0; PAGE_SIZE as usize]);

        let mut rx = self.page_status_rx.lock().await;
        let mut flush_count = 0;

        loop {
            let Ok((page_id, ref_type)) = rx.try_recv() else {
                info!("flushed {flush_count} pages");
                return Ok(());
            };

            let page_arc = self.in_use.get(&page_id).expect("page must exist");
            let ref_count = Arc::strong_count(&*page_arc);
            info!(?page_id, ?ref_count, "page ref count");

            // TODO: FIXME: This number (1) is wrong.
            if ref_count == 1 {
                // If strong count is 1, it was the last page reference.
                // Hence, it may be removed from the map.
                self.in_use.remove(&page_id);
            }

            if ref_type == PageRefType::Write {
                let mut buf = Buff::new(&mut *buf);

                {
                    // In write reads, this lock should never have any contention.
                    // ----------- fixme debug_assert_eq!(ref_count, 1);
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
                    info!(?page_id, "flushed page to disk");
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
    #[instrument(skip_all)]
    pub async fn alloc<S: SpecificPage>(&self) -> DbResult<PagerGuard<S>> {
        let ty = S::ty();
        info!(?ty, "allocating page");

        let p1g = self.get::<FirstPage>(PageId::new_u32(1)).await?;
        let mut p1 = p1g.write().await;

        p1.header.page_count += 1;

        let id = PageId::new_u32(p1.header.page_count);
        let init = S::default_with_id(id);

        // SAFETY: Metadata was changed above.
        unsafe { self.flush_page(&init) }.await?;

        info!("flushing first page metadata...");
        p1.flush();

        let guard_inner = Arc::new(RwLock::new(init.into_page()));
        self.in_use.insert(id, Arc::clone(&guard_inner));
        info!(?id, "page allocated");
        Ok(PagerGuard {
            inner: guard_inner,
            notifier: self.page_status_tx.clone(),
            _specific: PhantomData,
        })
    }

    /// Writes the given page to the database, skipping *all* cache layers.
    ///
    /// Most of the times, this method shouldn't be used directly. Instead,
    /// prefer the safer `alloc` method.
    ///
    /// # Safety
    ///
    /// The caller must ensure the database consistency. For example, this
    /// method doesn't update the metadata in the first page.
    pub async unsafe fn flush_page(&self, page: &impl SpecificPage) -> DbResult<()> {
        // TODO: Use a buffer pool.
        let mut buf = Box::new([0; PAGE_SIZE as usize]);
        let mut buf = Buff::new(&mut *buf);

        page.serialize(&mut buf)?;

        info!("flushing new page...");
        // Does one need to flush here if the page will be modified again
        // after the allocation? The rationale for flushing here is that the
        // first page was flushed to reflect the just-allocated page. Hence,
        // one needs to flush such a new page.
        self.disk_manager
            .lock() // <--- I think this *may* deadlock. :thinking:
            .await
            .write_page(page.id(), buf.get())
            .await
    }

    /// Clears all cache information associated with the given page ID.
    ///
    /// # Safety
    ///
    /// Callers must ensure that there are no other alive references to the
    /// given page.
    pub async unsafe fn clear_cache(&self, page_id: PageId) {
        self.in_use.remove(&page_id);
        self.cache.evict(&page_id).await;
    }

    /// Loads the page from the disk.
    async fn disk_read_page(&self, page_id: PageId) -> DbResult<Page> {
        // TODO: Use a buffer pool.
        let mut buf = Box::new([0; PAGE_SIZE as usize]);
        let mut buf = Buff::new(&mut *buf);

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
    pub async fn read(&self) -> PagerReadGuard<'_, S> {
        info!(ty = ?S::ty(), "acquiring read guard");
        PagerReadGuard {
            guard: self.inner.read().await,
            notifier: self.notifier.clone(),
            bomb: DropBomb::new("forgot to call `release` on pager read guard"),
            _specific: PhantomData,
        }
    }

    /// Locks the page for writing. There may be no other references (read or
    /// write) concurrently.
    pub async fn write(&self) -> PagerWriteGuard<'_, S> {
        info!(ty = ?S::ty(), "acquiring write guard");
        PagerWriteGuard {
            guard: self.inner.write().await,
            notifier: self.notifier.clone(),
            bomb: DropBomb::new("forgot to call `flush` on pager write guard"),
            _specific: PhantomData,
        }
    }
}

/// A page read guard. Non-exclusive for other read guards.
pub struct PagerReadGuard<'a, S> {
    guard: RwLockReadGuard<'a, Page>,
    notifier: PageNotificationSender,
    bomb: DropBomb,
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
        self.bomb.defuse();
        info!(ty = ?S::ty(), "released read guard");
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

/// A page write guard. Exclusive.
pub struct PagerWriteGuard<'a, S> {
    guard: RwLockWriteGuard<'a, Page>,
    notifier: PageNotificationSender,
    bomb: DropBomb,
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
        self.bomb.defuse();
        info!(ty = ?S::ty(), "flushed write guard");
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

/// The page reference type.
#[derive(Debug, PartialEq, Eq)]
enum PageRefType {
    Read,
    Write,
}
