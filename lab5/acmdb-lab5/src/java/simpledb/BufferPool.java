package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
//import java.util.concurrent.ConcurrentSkipListSet;
// import java.util.concurrent.locks.Condition;

/**
 * BufferPool 管理着从磁盘到内存中的页面读写操作。
 * 访问方法调用它来检索页面，并且它从适当的位置获取页面。
 * <p>
 * BufferPool 也负责锁定；当一个事务获取一个页面时，BufferPool 会检查该事务是否有适当的
 * 锁来读写页面。
 * 
 * @Threadsafe , 所有字段都是final
 */
public class BufferPool {
    /** 每个页面的字节数，包括头部。 */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /**
     * 传递给构造函数的默认页面数。这被其他类使用。
     * BufferPool 应该使用构造函数中的 numPages 参数代替。
     */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;

    private ConcurrentHashMap<TransactionId, Integer> waitingCount;
    private ConcurrentHashMap<PageId, Page> pageMap;
    // private ConcurrentSkipListSet<PageId> pagesNotLocked;
    private PageLock pageLock;
    private TransactionManager transactionManager;

    private class TransactionManager {
        private class TransactionInfo {
            public TransactionId tid;
            @SuppressWarnings("unused")
            public long startTime;
            public boolean mayDeadlock = false;
            public boolean aborting = false;
        }

        private ArrayList<TransactionInfo> transactionInfos;

        TransactionManager() {
            transactionInfos = new ArrayList<>();
        }

        public boolean isAborting(TransactionId tid) {
            synchronized (this) {
                for (TransactionInfo info : transactionInfos) {
                    if (info.tid.equals(tid)) {
                        return info.aborting;
                    }
                }
                return false;
            }
        }

        public boolean Abort(TransactionId tid) {
            synchronized (this) {
                for (TransactionInfo info : transactionInfos) {
                    if (info.tid.equals(tid)) {
                        info.aborting = true;
                        return true;
                    }
                }
                return false;
            }
        }

        public void addTransaction(TransactionId tid) {
            synchronized (this) {
                for (TransactionInfo info : transactionInfos) {
                    if (info.tid.equals(tid)) {
                        return;
                    }
                }
                TransactionInfo info = new TransactionInfo();
                info.tid = tid;
                info.startTime = System.currentTimeMillis();
                info.mayDeadlock = false;
                info.aborting = false;
                transactionInfos.add(info);
            }
        }

        public void removeTransaction(TransactionId tid) {
            synchronized (this) {
                for (TransactionInfo info : transactionInfos) {
                    if (info.tid.equals(tid)) {
                        transactionInfos.remove(info);
                        return;
                    }
                }
            }
        }

        public void setMayDeadlock(TransactionId tid, boolean val) {
            synchronized (this) {
                for (TransactionInfo info : transactionInfos) {
                    if (info.tid.equals(tid)) {
                        info.mayDeadlock = val;
                        return;
                    }
                }
            }
        }

        public boolean shouldAbort(TransactionId tid, int num) {
            synchronized (this) {
                // 如果在倒数前num个mayDeadLock中，则返回true
                for (int i = transactionInfos.size() - 1, count = 0; i >= 0 && count < num; --i) {
                    if (transactionInfos.get(i).tid.equals(tid)) {
                        if (transactionInfos.get(i).mayDeadlock) {
                            for (int j = i - 1; j >= 0; --j) {
                                if (transactionInfos.get(j).mayDeadlock) {
                                    return true;
                                }
                            }
                            return false;
                        } else {
                            return false;
                        }
                    }
                    if (transactionInfos.get(i).mayDeadlock) {
                        ++count;
                    }
                }
                return false;
            }
        }
    }

    private class PageLock {
        private ConcurrentHashMap<PageId, PageReadWriteLock> lockMap;
        private ConcurrentHashMap<TransactionId, Set<PageId>> tidMap;

        private class PageReadWriteLock {
            private SimpleMutex lock;
            private SimpleMutex rlock;
            private SimpleMutex wlock;
            private Semaphore writeLock;
            private Semaphore condition;
            private int readCount;
            private ConcurrentHashMap<TransactionId, Integer> tidLock;
            // static final int UNLOCK = 0;
            static final int READ_LOCK = 1;
            static final int WRITE_LOCK = -1;
            static final int WAIT_UPGRADE = -2;

            private class SimpleMutex {
                private Semaphore lock;

                public SimpleMutex() {
                    lock = new Semaphore(1);
                }

                public void lock() {
                    try {
                        lock.acquire();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                public void lock_with_interrupt() throws InterruptedException {
                    lock.acquire();
                }

                public void unlock() {
                    lock.release();
                }
            }

            public PageReadWriteLock() {
                rlock = new SimpleMutex();
                wlock = new SimpleMutex();
                lock = new SimpleMutex();
                writeLock = new Semaphore(1);
                readCount = 0;
                tidLock = new ConcurrentHashMap<>();
                condition = new Semaphore(0);
            }

            public void waitReadLock(TransactionId tid) {
                wlock.lock();
                rlock.lock();
                if (tidLock.containsKey(tid) && tidLock.get(tid) == WRITE_LOCK) {
                    wlock.unlock();
                    rlock.unlock();
                    return;
                }
                wlock.unlock();
                rlock.unlock();
                try {
                    lock.lock_with_interrupt();
                } catch (InterruptedException e) {
                    return;
                }
                try {
                    rlock.lock();
                    if (readCount == 0) {
                        try {
                            writeLock.acquire();
                        } catch (InterruptedException e) {
                            // should not happend
                            lock.unlock();
                            return;
                        }
                    }
                    readCount++;
                    // tidLock[tid]加一
                    tidLock.merge(tid, 1, Integer::sum);
                    rlock.unlock();
                } finally {
                    lock.unlock();
                }
            }

            public void waitWriteLock(TransactionId tid) {
                wlock.lock();
                rlock.lock();
                if (tidLock.containsKey(tid)) {
                    if (tidLock.get(tid).equals(WRITE_LOCK)) {
                        rlock.unlock();
                        wlock.unlock();
                        return;
                    }
                    if (tidLock.get(tid) == WAIT_UPGRADE - 1) {
                        rlock.unlock();
                        wlock.unlock();
                        System.exit(1);
                        return;
                    }
                }
                rlock.unlock();

                try {
                    lock.lock_with_interrupt();
                } catch (InterruptedException e) {
                    wlock.unlock();
                    return;
                }

                try {
                    rlock.lock();
                    if (tidLock.containsKey(tid) && tidLock.get(tid) >= READ_LOCK) {
                        if (tidLock.get(tid) == readCount) {
                            readCount = 0;
                            tidLock.put(tid, WRITE_LOCK);
                            rlock.unlock();
                            wlock.unlock();
                            return;
                        } else {
                            int numAcquired = readCount - tidLock.get(tid);
                            int tmp = tidLock.get(tid);
                            tidLock.put(tid, WAIT_UPGRADE);
                            condition.drainPermits();
                            rlock.unlock();
                            tidLock.put(tid, WAIT_UPGRADE - 1);
                            try {
                                condition.acquire(numAcquired);
                            } catch (Throwable e) {
                                if (!(e instanceof InterruptedException)) {
                                    System.exit(1);
                                }
                                condition.release(numAcquired);
                                tidLock.put(tid, tmp);
                                lock.unlock();
                                wlock.unlock();
                                return;
                            }
                            tidLock.put(tid, WAIT_UPGRADE - 2);
                            rlock.lock();
                            if (tmp != readCount) {
                                throw new RuntimeException("error");
                            }
                            readCount = 0;
                            tidLock.put(tid, WRITE_LOCK);
                            rlock.unlock();
                            wlock.unlock();
                            return;
                        }
                    }
                    rlock.unlock();
                    writeLock.acquire();
                } catch (Exception e) {
                    if (!(e instanceof InterruptedException)) {
                        System.exit(1);
                    }
                    wlock.unlock();
                    lock.unlock();
                    e.printStackTrace();
                    return;
                }
                rlock.lock();
                tidLock.put(tid, WRITE_LOCK);
                rlock.unlock();
                wlock.unlock();
            }

            public void releaseLock(TransactionId tid) {
                rlock.lock();
                if (tidLock.containsKey(tid) && tidLock.get(tid) >= READ_LOCK) {
                    readCount--;
                    tidLock.merge(tid, -1, Integer::sum);
                    if (tidLock.get(tid) == 0) {
                        tidLock.remove(tid);
                    }
                    if (readCount == 0) {
                        writeLock.release();
                    } else {
                        condition.release();
                    }
                } else if (tidLock.containsKey(tid) && tidLock.get(tid) == WRITE_LOCK) {
                    writeLock.release();
                    tidLock.remove(tid);
                    lock.unlock();
                }
                rlock.unlock();
            }

            public void releaseAllLock(TransactionId tid) {
                rlock.lock();
                if (tidLock.containsKey(tid) && tidLock.get(tid) >= READ_LOCK) {
                    int count = tidLock.remove(tid);
                    readCount -= count;
                    if (readCount == 0) {
                        writeLock.release();
                    } else {
                        condition.release(count);
                    }
                } else if (tidLock.containsKey(tid) && tidLock.get(tid) == WRITE_LOCK) {
                    writeLock.release();
                    tidLock.remove(tid);
                    lock.unlock();
                }
                rlock.unlock();
            }
        }

        private PageLock() {
            lockMap = new ConcurrentHashMap<>();
            tidMap = new ConcurrentHashMap<>();
        }

        private PageReadWriteLock getLock(PageId pid) {
            if (!lockMap.containsKey(pid)) {
                lockMap.put(pid, new PageReadWriteLock());
            }
            return lockMap.get(pid);
        }

        // private boolean tryLock(PageId pid, Permissions perm) {
        // PageReadWriteLock lock = getLock(pid);
        // if (perm == Permissions.READ_ONLY) {
        // return lock.readLock().tryLock();
        // } else {
        // return lock.writeLock().tryLock();
        // }
        // }

        private void acquireLock(TransactionId tid, PageId pid, Permissions perm) {
            synchronized (this) {
                if (!tidMap.containsKey(tid)) {
                    tidMap.put(tid, new CopyOnWriteArraySet<>());
                    transactionManager.addTransaction(tid);
                }
            }
            PageReadWriteLock lock = getLock(pid);
            // lock.waitWriteLock(tid);
            if (perm == Permissions.READ_ONLY) {
                lock.waitReadLock(tid);
            } else {
                lock.waitWriteLock(tid);
            }
            try {
                tidMap.get(tid).add(pid);
            } catch (Exception e) {
                e.printStackTrace();
            }
            // Set<PageId> s = tidMap.get(tid);
        }

        private void releaseLock(TransactionId tid) {
            if (!tidMap.containsKey(tid)) {
                return;
            }
            for (PageId pid : tidMap.get(tid)) {
                PageReadWriteLock lock = getLock(pid);
                lock.releaseLock(tid);
            }
            tidMap.remove(tid);
        }

        private void releaseAllLock(TransactionId tid) {
            if (!tidMap.containsKey(tid)) {
                return;
            }
            for (PageId pid : tidMap.get(tid)) {
                PageReadWriteLock lock = getLock(pid);
                lock.releaseAllLock(tid);
            }
            tidMap.remove(tid);
        }

        private void upgradeToWriteLock(TransactionId tid, PageId pid) {
            PageReadWriteLock lock = getLock(pid);
            lock.waitWriteLock(tid);
        }

        private void releaseLock(TransactionId tid, PageId pid) {
            if (!lockMap.containsKey(pid)) {
                return;
            }
            PageReadWriteLock lock = lockMap.get(pid);
            lock.releaseLock(tid);
        }

        private Set<PageId> getPages(TransactionId tid) {
            return tidMap.get(tid);
        }

        private boolean holdsLock(TransactionId tid, PageId pid) {
            if (!tidMap.containsKey(tid)) {
                return false;
            }
            return tidMap.get(tid).contains(pid);
        }

        private void clearTid(TransactionId tid) {
            tidMap.remove(tid);
        }
    }

    /**
     * 创建一个最多缓存 numPages 个页面的 BufferPool。
     *
     * @param numPages 这个缓冲池中的最大页面数。
     */
    public BufferPool(int numPages) {
        pageMap = new ConcurrentHashMap<>();
        // pagesNotLocked = new ConcurrentSkipListSet<>();
        this.numPages = numPages;
        pageLock = new PageLock();
        transactionManager = new TransactionManager();
        waitingCount = new ConcurrentHashMap<>();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // 此函数仅供测试使用!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // 此函数仅供测试使用!!
    public static void resetPageSize() {
        BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * 根据相关权限检索指定的页面。
     * 将获取一个锁，如果该锁被另一个事务持有，则可能会阻塞。
     * <p>
     * 检索到的页面应该在缓冲池中查找。如果存在，应该返回。如果不存在，应该
     * 加入到缓冲池中并返回。如果缓冲池中空间不足，应该逐出一个页面并将新页面
     * 加入其位置。
     *
     * @param tid  请求页面的事务的 ID
     * @param pid  请求的页面的 ID
     * @param perm 页面上请求的权限
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        if (transactionManager.isAborting(tid)) {
            throw new TransactionAbortedException();
        }
        // 处理锁
        boolean doneFlag = false;
        Random random = new Random();
        waitingCount.merge(tid, 1, Integer::sum);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        for (int basic_time = 200, max_waiting_time = basic_time; max_waiting_time <= 1024 * basic_time; max_waiting_time *= 2) {
            Future<Void> future = null;
            try {
                future = executor.submit(() -> {
                    pageLock.acquireLock(tid, pid, perm);
                    return null;
                });
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(1);
            }
            try {
                int actual_waiting_time = max_waiting_time + random.nextInt(max_waiting_time / 10);
                future.get(actual_waiting_time, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                try {
                    if (max_waiting_time >= 400) {
                        transactionManager.setMayDeadlock(tid, true);
                    }
                    int numAbort = max_waiting_time / basic_time;
                    if (transactionManager.isAborting(tid) ||
                            max_waiting_time >= basic_time * 1024 ||
                            transactionManager.shouldAbort(tid, numAbort)) {
                        // waitingCount.merge(tid, -1, Integer::sum);
                        if (!future.cancel(true)) {
                            System.exit(1);
                        }
                        waitingCount.merge(tid, -1, Integer::sum);
                        executor.shutdownNow();
                        while (!executor.awaitTermination(1, TimeUnit.MICROSECONDS)) {
                            executor.shutdownNow();
                        }
                        throw new TransactionAbortedException();
                    }
                    if (!future.cancel(true)) {
                        System.exit(1);
                    }
                    continue;
                } catch (TransactionAbortedException e1) {
                    throw e1;
                } catch (Throwable e2) {
                    e2.printStackTrace();
                    System.exit(1);
                }
            } catch (InterruptedException | ExecutionException e) {
                if (!future.cancel(true)) {
                    System.exit(1);
                }
                waitingCount.merge(tid, -1, Integer::sum);
                executor.shutdown();
                throw new TransactionAbortedException();
            } catch(Throwable e) {
                future.cancel(true);
                e.printStackTrace();
                System.exit(1);
            } finally {
                // do nothing
            }
            if (!future.isDone()) {
                // should not happend
                System.exit(1);
            }
            future.cancel(true);
            waitingCount.merge(tid, -1, Integer::sum);
            executor.shutdown();
            transactionManager.setMayDeadlock(tid, false);
            doneFlag = true;
            break;
        }
        if (!doneFlag) {
            throw new TransactionAbortedException();
        }
        // throw new TransactionAbortedException();
        // // 如果pid在pagesNotLocked中，就删除
        // pagesNotLocked.remove(pid);
        Page page = pageMap.get(pid);
        if (page == null) {
            if (pageMap.size() >= numPages) {
                evictPage();
            }
            Catalog catalog = Database.getCatalog();
            DbFile file = catalog.getDatabaseFile(pid.getTableId());
            page = file.readPage(pid);
            pageMap.put(pid, page);
        }
        return page;
    }

    // public void upgradeToWriteLock(TransactionId tid, PageId pid) throws
    // TransactionAbortedException, DbException {
    // getPage(tid, pid, Permissions.READ_WRITE);
    // }

    /**
     * 释放页面上的锁。
     * 调用这个是非常危险的，可能会导致错误的行为。仔细考虑谁需要调用这个以及为什么，
     * 以及他们为什么可以承担调用它的风险。
     *
     * @param tid 请求解锁的事务的 ID
     * @param pid 要解锁的页面的 ID
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // 一些代码在这里
        // lab1|lab2 不需要
        pageLock.releaseLock(tid, pid);
    }

    /**
     * 释放与给定事务相关联的所有锁。
     *
     * @param tid 请求解锁的事务的 ID
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // 一些代码在这里
        // lab1|lab2 不需要
        transactionComplete(tid, true);
    }

    /** 如果指定事务在指定页面上有锁，则返回 true */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // 一些代码在这里
        // lab1|lab2 不需要
        return pageLock.holdsLock(tid, p);
    }

    public void waitAbort(TransactionId tid) {
        // 每隔10ms检查一次
        while (waitingCount.get(tid) != null && waitingCount.get(tid) > 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 提交或中止给定事务；释放事务相关的所有锁。
     *
     * @param tid    请求解锁的事务的 ID
     * @param commit 标志是否应该提交或中止
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // 一些代码在这里
        // lab1|lab2 不需要
        if (commit) {
            flushPages(tid);
        } else {
            discardPages(tid);
            transactionManager.Abort(tid);
        }
        waitAbort(tid);
        pageLock.releaseAllLock(tid);
        transactionManager.removeTransaction(tid);
        pageLock.clearTid(tid);
    }

    /**
     * 代表事务 tid 向指定表中添加一个元组。将获取页面上添加元组的写锁以及任何
     * 其他更新的页面的写锁（lab2 不需要锁获取）。
     * 如果无法获取锁，则可能会阻塞。
     * 
     * 通过调用它们的 markDirty 位，将操作弄脏的任何页面标记为脏，并将
     * 弄脏的任何页面的版本添加到缓存中（替换这些页面的任何现有版本），
     * 以便未来的请求看到最新的页面。
     *
     * @param tid     添加元组的事务
     * @param tableId 要添加元组的表
     * @param t       要添加的元组
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // 一些代码在这里
        // lab1 不需要
        DbFile db = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = db.insertTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
            pageMap.put(page.getId(), page);
        }
    }

    /**
     * 从缓冲池中删除指定的元组。
     * 将获取元组被删除的页面上的写锁和任何其他更新的页面的写锁。
     * 如果无法获取锁，则可能会阻塞。
     *
     * 通过调用它们的 markDirty 位，将操作弄脏的任何页面标记为脏，并将
     * 弄脏的任何页面的版本添加到缓存中（替换这些页面的任何现有版本），
     * 以便未来的请求看到最新的页面。
     *
     * @param tid 删除元组的事务。
     * @param t   要删除的元组
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // 一些代码在这里
        // lab1 不需要
        DbFile db = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pages = db.deleteTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
            pageMap.put(page.getId(), page);
        }
    }

    /**
     * 将所有脏页面刷新到磁盘。
     * 注意：小心使用这个例程——它将脏数据写入磁盘，因此如果在 NO STEAL 模式下运行，将会破坏 simpledb。
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : pageMap.keySet()) {
            flushPage(pid);
        }
    }

    /**
     * 从缓冲池中移除特定的页面 id。
     * 恢复管理器需要此操作以确保缓冲池不会在其缓存中保留已回滚的页面。
     * 
     * B+树文件也使用它来确保删除的页面从缓存中移除，以便可以安全地重用这些页面。
     */
    public synchronized void discardPage(PageId pid) {
        // 一些代码在这里
        // lab1 不需要
        if (pageMap.containsKey(pid)) {
            // try {
            // flushPage(pid);
            // } catch (IOException e) {
            // e.printStackTrace();
            // }
            pageMap.remove(pid);
        }
    }

    /**
     * 从缓冲池中移除特定的页面 id。
     * 恢复管理器需要此操作以确保缓冲池不会在其缓存中保留已回滚的页面。
     * 
     * B+树文件也使用它来确保删除的页面从缓存中移除，以便可以安全地重用这些页面。
     */
    public synchronized void discardPages(TransactionId tid) {
        // 一些代码在这里
        // lab1 不需要
        Set<PageId> pages = pageLock.getPages(tid);
        for (PageId pid : pages) {
            discardPage(pid);
        }
    }

    /**
     * 将某个特定页面刷新到磁盘
     * 
     * @param pid 表示要刷新的页面的 ID
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // 一些代码在这里
        // 获取锁
        if (pageMap.containsKey(pid)) {
            Page page = pageMap.get(pid);
            if (page.isDirty() != null) {
                Catalog catalog = Database.getCatalog();
                DbFile file = catalog.getDatabaseFile(pid.getTableId());
                file.writePage(page);
                page.markDirty(false, null);
            }
        }
    }

    private synchronized boolean isDirty(PageId pid) {
        if (pageMap.containsKey(pid)) {
            return pageMap.get(pid).isDirty() != null;
        }
        return false;
    }

    /**
     * 将指定事务的所有页面写入磁盘。
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // 一些代码在这里
        // lab1|lab2 不需要
        Set<PageId> pages = pageLock.getPages(tid);
        if (pages == null) {
            return;
        }
        for (PageId pid : pages) {
            flushPage(pid);
        }
    }

    /**
     * 从缓冲池中丢弃一个页面。
     * 将页面刷新到磁盘以确保脏页面在磁盘上更新。
     */
    private synchronized void evictPage() throws DbException {
        // 一些代码在这里
        // lab1 不需要
        for (PageId pid : pageMap.keySet()) {
            if (isDirty(pid)) {
                continue;
            }
            // try {
            // flushPage(pid);
            // } catch (IOException e) {
            // e.printStackTrace();
            // }
            pageMap.remove(pid);
            return;
        }
        throw new DbException("run out of buffer pool");
        // for (int tryTimes = 1; tryTimes < 10; ++tryTimes) {
        // for (PageId pid : pagesNotLocked) {
        // // 尝试获取锁，失败就continue
        // if (!pageLock.tryLock(pid, Permissions.READ_WRITE)) {
        // continue;
        // }
        // try {
        // flushPage(pid);
        // pageMap.remove(pid);
        // } catch (IOException e) {
        // e.printStackTrace();
        // } finally {
        // pageLock.releaseWriteLock(pid);
        // }
        // return;
        // }
        // }
        // throw new DbException("cannot evict page from buffer pool");
    }

}