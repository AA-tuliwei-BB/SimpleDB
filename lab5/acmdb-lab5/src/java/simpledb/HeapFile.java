package simpledb;

import static org.junit.Assert.assertArrayEquals;

import java.io.*;
import java.util.*;

/**
 * HeapFile是DbFile的一种实现，它存储了一系列以无特定顺序排列的元组。
 * 元组存储在页面上，每个页面都有固定的大小，文件就是这些页面的集合。
 * HeapFile与HeapPage紧密合作。HeapPage的格式在HeapPage构造函数中有描述。
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    private RandomAccessFile raf;

    /**
     * 构造一个由指定文件支持的堆文件。
     * 
     * @param f
     *            存储这个堆文件的磁盘后备存储的文件。
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        try {
            this.raf = new RandomAccessFile(f, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 返回支持此HeapFile在磁盘上的文件。
     * 
     * @return 支持此HeapFile在磁盘上的文件。
     */
    public File getFile() {
        return f;
    }

    /**
     * 返回唯一标识这个HeapFile的ID。实现注意：
     * 你需要在某处生成这个表id，确保每个HeapFile都有一个“唯一id”，
     * 并且对于特定的HeapFile总是返回相同的值。我们建议哈希HeapFile底层文件的绝对文件名，
     * 即 f.getAbsoluteFile().hashCode()。
     * 
     * @return 唯一标识这个HeapFile的ID。
     */
    public int getId() {
        return f.getAbsolutePath().hashCode();
    }

    /**
     * 返回存储在这个DbFile中的表的TupleDesc。
     * 
     * @return 这个DbFile的TupleDesc。
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // 见DbFile.java中的javadocs
    public Page readPage(PageId pid) {
        if (pid.getTableId() != this.getId()) {
            return null;
        }
        try {
            raf.seek(BufferPool.getPageSize() * pid.pageNumber());
            byte[] buffer = new byte[BufferPool.getPageSize()];
            raf.read(buffer);
            return new HeapPage((HeapPageId)pid, buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 见DbFile.java中的javadocs
    public void writePage(Page page) throws IOException {
        // 一些代码在这里
        if (page.getId().getTableId() != this.getId()) {
            throw new IOException("table id mismatch");
        }
        raf.seek(BufferPool.getPageSize() * page.getId().pageNumber());
        raf.write(page.getPageData());
    }

    /**
     * 返回这个HeapFile中的页面数量。
     */
    public int numPages() {
        int pageSie = BufferPool.getPageSize();
        return (int) (f.length() + pageSie - 1) / pageSie;
    }

    // 见DbFile.java中的javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // 一些代码在这里
        ArrayList<Page> affectedPages = new ArrayList<>();
        // 不加 i < numPages() 了是因为将创建新页的任务放在了Bufferpool中以保证事务的正确性
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() > 0) {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                // Database.getBufferPool().upgradeToWriteLock(tid, pid);
                page.insertTuple(t);
                affectedPages.add(page);
                return affectedPages;
            } else {
                Database.getBufferPool().releasePage(tid, pid);
            }
        }
        synchronized (this) {
            HeapPageId pid = new HeapPageId(getId(), numPages());
            HeapPage page = new HeapPage(pid, HeapPage.createEmptyPageData());
            writePage(page);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            page.insertTuple(t);
            affectedPages.add(page);
            return affectedPages;
        }
    }

    // 见DbFile.java中的javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // 一些代码在这里
        ArrayList<Page> affectedPages = new ArrayList<>();
        RecordId rid = t.getRecordId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, rid.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        affectedPages.add(page);
        return affectedPages;
    }

    // 见DbFile.java中的javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            private boolean closed = true;
            private int nextPage = 0;
            private HeapPage page;
            private Iterator<Tuple> tuples;

            /**
             * Opens the iterator
             * 
             * @throws DbException when there are problems opening/accessing the database.
             */
            public void open() throws DbException, TransactionAbortedException {
                closed = false;
                nextPage = 0;
                page = null;
                tuples = null;
            }

            /**
             * @return true if there are more tuples available, false if no more tuples or
             *         iterator isn't open.
             */
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (closed)
                    return false;
                while (tuples == null || tuples.hasNext() == false) {
                    if (nextPage >= numPages()) {
                        return false;
                    } else {
                        page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), nextPage), Permissions.READ_ONLY);
                        tuples = page.iterator();
                        nextPage++;
                    }
                }
                return true;
            }

            /**
             * Gets the next tuple from the operator (typically implementing by reading
             * from a child operator or an access method).
             *
             * @return The next tuple in the iterator.
             * @throws NoSuchElementException if there are no more tuples
             */
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return tuples.next();
            }

            /**
             * Resets the iterator to the start.
             * 
             * @throws DbException When rewind is unsupported.
             */
            public void rewind() throws DbException, TransactionAbortedException {
                nextPage = 0;
                tuples = null;
                page = null;
            }

            /**
             * Closes the iterator.
             */
            public void close() {
                closed = true;
                page = null;
                tuples = null;
            }
        };
    }

}
