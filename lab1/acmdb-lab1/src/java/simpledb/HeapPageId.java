package simpledb;

/** 堆页面对象的唯一标识符。 */
public class HeapPageId implements PageId {

    private final int tableId;
    private final int pgNo;

    /**
     * 构造函数。为特定表的特定页面创建一个页面id结构。
     *
     * @param tableId 被引用的表
     * @param pgNo 该表中的页面号。
     */
    public HeapPageId(int tableId, int pgNo) {
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /** @return 与此PageId关联的表 */
    public int getTableId() {
        return tableId;
    }

    /**
     * @return 与此PageId关联的getTableId()表中的页面号
     */
    public int pageNumber() {
        return pgNo;
    }

    /**
     * @return 此页面的哈希码，由表号和页面号的连接表示
     *   （例如，如果PageId被用作BufferPool中哈希表的键，则需要。）
     * @see BufferPool
     */
    public int hashCode() {
        return tableId * 31 + pgNo;
    }

    /**
     * 将一个PageId与另一个PageId进行比较。
     *
     * @param o 要比较的对象（必须是PageId）
     * @return 如果对象相等（例如，页面号和表id相同）则返回true
     */
    public boolean equals(Object o) {
        if (o == null || o.getClass() != this.getClass()) {
            return false;
        }
        PageId pid = (PageId) o;
        return pid.getTableId() == tableId && pid.pageNumber() == pgNo;
    }

    /**
     * 将此对象的表示形式返回为整数数组，以便写入磁盘。返回的数组大小必须包含
     * 与构造函数之一的参数数量相对应的整数数量。
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
