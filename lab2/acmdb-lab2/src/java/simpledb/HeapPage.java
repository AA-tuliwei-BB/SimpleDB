package simpledb;

import java.util.*;
import java.io.*;

/**
 * HeapPage的每个实例存储一个HeapFiles页面的数据，并实现了由BufferPool使用的Page接口。
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock = new Byte((byte) 0);

    /**
     * 从磁盘读取的一组字节数据中创建一个HeapPage。
     * HeapPage的格式是一组头部字节，指示页面中的哪些槽正在使用，以及一些数量的元组槽。
     * 具体来说，元组的数量等于：
     * <p>
     * floor((BufferPool.getPageSize()*8) / (元组大小 * 8 + 1))
     * <p>
     * 其中元组大小是这个数据库表中元组的大小，可以通过 {@link Catalog#getTupleDesc} 确定。
     * 8位头部字的数量等于：
     * <p>
     * ceiling(元组槽数 / 8)
     * <p>
     * 
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // 为这个页面分配并读取头部槽
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // 为这个页面分配并读取实际的记录
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * 检索此页面上的元组数量。
     * 
     * @return 此页面上的元组数量
     */
    private int getNumTuples() {
        // floor((BufferPool.getPageSize()*8) / (元组大小 * 8 + 1))
        return (BufferPool.getPageSize() * 8) / (td.getSize() * 8 + 1);
    }

    /**
     * 计算一个HeapFile页面头部的字节数，每个元组占用tupleSize字节
     * 
     * @return 一个HeapFile页面头部的字节数，每个元组占用tupleSize字节
     */
    private int getHeaderSize() {
        // ceiling(元组槽数 / 8)
        return (numSlots + 7) / 8;
    }

    /**
     * 返回此页面修改前的视图
     * -- 由恢复过程使用
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            // 永远不应该发生 -- 我们之前已经成功解析过了！
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return 与此页面关联的PageId。
     */
    public HeapPageId getId() {
        return pid;
    }

    /**
     * 从源文件吸取元组。
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // 如果相关位没有设置，向前读取到下一个元组，并返回null。
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // 读取元组中的字段
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * 生成代表此页面内容的字节数组。
     * 用于将此页面序列化到磁盘。
     * <p>
     * 这里的不变性是，应该可以将getPageData生成的字节数组传递给HeapPage构造函数，
     * 并产生一个相同的HeapPage对象。
     *
     * @see #HeapPage
     * @return 对应于此页面字节的字节数组。
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // 创建页面的头部
        for (int i = 0; i < header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // 真的不应该发生
                e.printStackTrace();
            }
        }

        // 创建元组
        for (int i = 0; i < tuples.length; i++) {

            // 空槽
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // 非空槽
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // 填充
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); // - numSlots *
                                                                                                 // td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * 静态方法，生成对应于空HeapPage的字节数组。
     * 用于向文件添加新的、空的页面。将这个方法的结果传递给HeapPage构造函数会创建一个没有有效元组的HeapPage。
     *
     * @return 返回的ByteArray。
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; // 全部为0
    }

    /**
     * 从页面中删除指定的元组；元组应该更新以反映它不再存储在任何页面上。
     * 
     * @throws DbException 如果此元组不在此页面上，或者元组槽已经为空。
     * @param t 要删除的元组
     */
    public void deleteTuple(Tuple t) throws DbException {
        // 一些代码在这里
        // lab1 不需要
    }

    /**
     * 将指定的元组添加到页面上；元组应该更新以反映它现在存储在此页面上。
     * 
     * @throws DbException 如果页面已满（没有空槽）或tupledesc不匹配。
     * @param t 要添加的元组。
     */
    public void insertTuple(Tuple t) throws DbException {
        // 一些代码在这里
        // lab1 不需要
    }

    /**
     * 标记此页面为脏/非脏，并记录做脏它的事务
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // 一些代码在这里
        // lab1 不需要
    }

    /**
     * 返回最后使此页面脏的事务的tid，如果页面不脏则返回null
     */
    public TransactionId isDirty() {
        // 一些代码在这里
        // lab1 不需要
        return null;
    }

    /**
     * 返回此页面上空槽的数量。
     */
    public int getNumEmptySlots() {
        int result = 0;
        for (int i = 0; i < numSlots; ++i) {
            if (!isSlotUsed(i)) {
                result++;
            }
        }
        return result;
    }

    /**
     * 如果此页面上的相关槽已填充，则返回true。
     */
    public boolean isSlotUsed(int i) {
        return (header[i / 8] & (1 << (i & 7))) != 0;
    }

    /**
     * 在此页面上填充或清除一个槽的抽象方法。
     */
    private void markSlotUsed(int i, boolean value) {
        if (isSlotUsed(i) == value) {
            return;
        } else {
            header[i / 8] ^= 1 << (i & 7);
        }
        // lab1 不需要
    }

    /**
     * @return 一个遍历此页面上所有元组的迭代器（在这个迭代器上调用remove会抛出UnsupportedOperationException）
     *         （注意，这个迭代器不应该返回空槽中的元组！）
     */
    public Iterator<Tuple> iterator() {
        return new Iterator<Tuple>() {
            private int index = 0;
            
            @Override
            public boolean hasNext() {
                while (index < numSlots && !isSlotUsed(index)) {
                    index++;
                }
                return index < numSlots;
            }

            @Override
            public Tuple next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return tuples[index++];
            }

            @Override
            public void remove() throws UnsupportedOperationException {
                throw new UnsupportedOperationException();
            }
        };
    }

}
