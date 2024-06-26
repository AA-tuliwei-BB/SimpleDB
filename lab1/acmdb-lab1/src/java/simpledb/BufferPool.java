package simpledb;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;

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

    private ConcurrentHashMap<PageId, Page> pageMap;

    /**
     * 创建一个最多缓存 numPages 个页面的 BufferPool。
     *
     * @param numPages 这个缓冲池中的最大页面数。
     */
    public BufferPool(int numPages) {
        pageMap = new ConcurrentHashMap<>();
        this.numPages = numPages;
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
        Page page = pageMap.get(pid);
        if (page == null) {
            if (pageMap.size() >= numPages) {
                throw new DbException("buffer pool full");
            } else {
                Catalog catalog = Database.getCatalog();
                DbFile file = catalog.getDatabaseFile(pid.getTableId());
                page = file.readPage(pid);
                pageMap.put(pid, page);
            }
        }
        return page;
    }

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
    }

    /**
     * 释放与给定事务相关联的所有锁。
     *
     * @param tid 请求解锁的事务的 ID
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // 一些代码在这里
        // lab1|lab2 不需要
    }

    /** 如果指定事务在指定页面上有锁，则返回 true */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // 一些代码在这里
        // lab1|lab2 不需要
        return false;
    }

    /**
     * 提交或中止给定事务；释放事务相关的所有锁。
     *
     * @param tid    请求解锁的事务的 ID
     * @param commit 标志是否应该提交或中止
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // 一些代码在这里
        // lab1|lab2 不需要
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
    }

    /**
     * 将所有脏页面刷新到磁盘。
     * 注意：小心使用这个例程——它将脏数据写入磁盘，因此如果在 NO STEAL 模式下运行，将会破坏 simpledb。
     */
    public synchronized void flushAllPages() throws IOException {
        // 一些代码在这里
        // lab1 不需要

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
    }

    /**
     * 将某个特定页面刷新到磁盘
     * 
     * @param pid 表示要刷新的页面的 ID
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // 一些代码在这里
        // lab1 不需要
    }

    /**
     * 将指定事务的所有页面写入磁盘。
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // 一些代码在这里
        // lab1|lab2 不需要
    }

    /**
     * 从缓冲池中丢弃一个页面。
     * 将页面刷新到磁盘以确保脏页面在磁盘上更新。
     */
    private synchronized void evictPage() throws DbException {
        // 一些代码在这里
        // lab1 不需要
    }

}