package simpledb;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final Map<PageId, Page> pool;
    private final int numPages;
    private final Map<PageId, ReadWriteSemaphore> lockMap;
    private final Map<TransactionPagePair, LockInfo> locks;

    private final DependencyGraph graph = new DependencyGraph();

    private static class DependencyGraph {
        private static class SemaWrite {
            public ReadWriteSemaphore semaphore;
            public boolean write;

            public SemaWrite(ReadWriteSemaphore semaphore, boolean write) {
                this.semaphore = semaphore;
                this.write = write;
            }
        }

        private static class TransactionWrite {
            public TransactionId tid;
            public boolean write;

            public TransactionWrite(TransactionId tid, boolean write) {
                this.tid = tid;
                this.write = write;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                TransactionWrite that = (TransactionWrite) o;

                if (write != that.write) return false;
                return tid.equals(that.tid);
            }

            @Override
            public int hashCode() {
                int result = tid.hashCode();
                result = 31 * result + (write ? 1 : 0);
                return result;
            }
        }

        private final Map<TransactionId, SemaWrite> tid2sema = new ConcurrentHashMap<>();
        private final Map<ReadWriteSemaphore, Set<TransactionWrite>> sema2tid = new ConcurrentHashMap<>();

        private boolean check(TransactionId start, TransactionId cur) {
            SemaWrite s = tid2sema.get(cur);
            if (s == null) return false;
            for (TransactionWrite t : sema2tid.getOrDefault(s.semaphore, new HashSet<>())) {
                if (!s.write && !t.write) continue;
                if (t.tid == cur) continue;
                if (t.tid == start || check(start, t.tid)) return true;
            }
            return false;
        }

        public synchronized boolean wait(TransactionId tid, ReadWriteSemaphore sema, boolean write) {
            tid2sema.put(tid, new SemaWrite(sema, write));
            if (check(tid, tid)) {
                tid2sema.remove(tid);
                return true;
            } else return false;
        }

        public synchronized void acquire(TransactionId tid, ReadWriteSemaphore sema, boolean write) {
            tid2sema.remove(tid);
            sema2tid.computeIfAbsent(sema, s -> new HashSet<>()).add(new TransactionWrite(tid, write));
        }

        public synchronized void release(TransactionId tid, ReadWriteSemaphore sema, boolean write) {
            Set<TransactionWrite> set = sema2tid.get(sema);
            set.remove(new TransactionWrite(tid, write));
            if (set.isEmpty()) sema2tid.remove(sema);
        }
    }

    private class ReadWriteSemaphore {
        private final Semaphore read = new Semaphore(1);
        private final Semaphore write = new Semaphore(1);
        private final Semaphore upgrade = new Semaphore(1);
        private int readCount = 0;

        public void lockRead(TransactionId tid) throws TransactionAbortedException {
            if (graph.wait(tid, this, false)) throw new TransactionAbortedException();
            read.acquireUninterruptibly();
            readCount++;
            if (readCount == 1) write.acquireUninterruptibly();
            read.release();
            graph.acquire(tid, this, false);
        }

        public void unlockRead(TransactionId tid) {
            read.acquireUninterruptibly();
            readCount--;
            if (readCount == 0) write.release();
            read.release();
            graph.release(tid, this, false);
        }

        public void upgrade(TransactionId tid) throws TransactionAbortedException {
            if (graph.wait(tid, this, true)) throw new TransactionAbortedException();
            read.acquireUninterruptibly();
            readCount--;
            // multiple upgrade results in deadlock in graph, so no deadlock may happen here
            upgrade.acquireUninterruptibly();
            if (readCount == 0) write.release();
            read.release();
            write.acquireUninterruptibly();
            upgrade.release();
            graph.release(tid, this, false);
            graph.acquire(tid, this, true);
        }

        public void lockWrite(TransactionId tid) throws TransactionAbortedException {
            if (graph.wait(tid, this, true)) throw new TransactionAbortedException();
            write.acquireUninterruptibly();
            graph.acquire(tid, this, true);
        }

        public void unlockWrite(TransactionId tid) {
            write.release();
            graph.release(tid, this, true);
        }
    }

    private static class LockInfo {
        private final TransactionId tid;
        private final ReadWriteSemaphore lock;
        private Boolean write;

        public LockInfo(TransactionId tid, ReadWriteSemaphore lock) {
            this.tid = tid;
            this.lock = lock;
            this.write = null;
        }

        public void update(boolean write) throws TransactionAbortedException {
            if (this.write == null && !write) {
                lock.lockRead(tid);
                this.write = false;
            } else if (this.write == null && write) {
                lock.lockWrite(tid);
                this.write = true;
            } else if (!this.write && write) {
                lock.upgrade(tid);
                this.write = true;
            }
        }

        public boolean isWrite() {
            return write != null && write;
        }

        public void unlock() {
            if (write != null) {
                if (write) lock.unlockWrite(tid);
                else lock.unlockRead(tid);
                write = null;
            }
        }
    }

    private static class TransactionPagePair {
        private final TransactionId tid;
        private final PageId pid;

        public TransactionId getTid() {
            return tid;
        }

        public PageId getPid() {
            return pid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TransactionPagePair that = (TransactionPagePair) o;

            if (!tid.equals(that.tid)) return false;
            return pid.equals(that.pid);
        }

        @Override
        public int hashCode() {
            int result = tid.hashCode();
            result = 31 * result + pid.hashCode();
            return result;
        }

        public TransactionPagePair(TransactionId tid, PageId pid) {
            this.tid = tid;
            this.pid = pid;
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pool = new ConcurrentHashMap<>();
        lockMap = new ConcurrentHashMap<>();
        locks = new ConcurrentHashMap<>();
        this.numPages = numPages;
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        Page ret = pool.get(pid);
        if (ret == null) {
            if (pool.size() == numPages) evictPage();
            ret = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pool.put(pid, ret);
        }
        ReadWriteSemaphore lock = lockMap.computeIfAbsent(pid, p -> new ReadWriteSemaphore());
        LockInfo info = locks.computeIfAbsent(new TransactionPagePair(tid, pid), p -> new LockInfo(tid, lock));
        info.update(perm == Permissions.READ_WRITE);
        return ret;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        LockInfo info = locks.get(new TransactionPagePair(tid, pid));
        if (info != null) {
            info.unlock();
            locks.remove(new TransactionPagePair(tid, pid));
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return locks.containsKey(new TransactionPagePair(tid, p));
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        if (commit) flushPages(tid);
        else {
            for (Map.Entry<TransactionPagePair, LockInfo> entry : locks.entrySet())
                if (entry.getKey().getTid().equals(tid) && entry.getValue().isWrite())
                    discardPage(entry.getKey().getPid());
        }
        for (Map.Entry<TransactionPagePair, LockInfo> entry : locks.entrySet())
            if (entry.getKey().getTid().equals(tid))
                entry.getValue().unlock();
    }

    private void ensureModifiedPages(Page page) throws DbException {
        if (!pool.containsKey(page.getId()) && pool.size() == numPages) evictPage();
        pool.put(page.getId(), page);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        for (Page p : file.insertTuple(tid, t)) {
            ensureModifiedPages(p);
            p.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        for (Page p : file.deleteTuple(tid, t)) {
            ensureModifiedPages(p);
            p.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public void flushAllPages() throws IOException {
        for (PageId pageId : new ArrayList<>(pool.keySet())) {
            flushPage(pageId);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public void discardPage(PageId pid) {
        pool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid) throws IOException {
        if (pool.containsKey(pid) && pool.get(pid).isDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pool.get(pid));
            pool.get(pid).markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public void flushPages(TransactionId tid) throws IOException {
        for (Map.Entry<TransactionPagePair, LockInfo> entry : locks.entrySet())
            if (entry.getKey().getTid().equals(tid) && entry.getValue().isWrite())
                flushPage(entry.getKey().getPid());
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        Iterator<Map.Entry<PageId, Page>> iterator = pool.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<PageId, Page> entry = iterator.next();
            if (entry.getValue().isDirty() != null) continue;
            try {
                flushPage(entry.getKey());
                iterator.remove();
                return;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new DbException("All pages are dirty");
    }

}
